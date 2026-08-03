#[allow(dead_code)]
mod support;

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use alloy_primitives::Bytes;
use alloy_sol_types::SolValue;
use anyhow::Result;
use axum::{Json, Router, extract::State, routing::post};
use bigname_ingest::{Engine, LiveBatchRequest, Marker, SourceDescriptor};
use bigname_lookup::ChainRpcUrls;
use bigname_project::Hydrator;
use phase_runner::{
    INTERPRETER_CONTENT_HASH,
    capacity::CapacityGuard,
    config::{CapacityConfig, ChainConfig, SeedBasis, SourceConfig, TimingConfig},
    error::RunnerError,
    heads::{BlockMarker, HeadMarkers, publish_heads},
    ingest_phase::IngestPhase,
    interpret_phase::InterpretPhase,
    live_phase::LivePhase,
    phase::{
        BlockRange, LoopbackPhase, Phase, PhaseContext, PhaseFuture, PhaseName, PhaseResume,
        PhaseSet, RunMode,
    },
    project_phase::ProjectPhase,
    rewind::rewind_to_ancestor,
    runner::{PhaseRunner, RedoPhase},
    state::PhaseStore,
};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::{net::TcpListener, sync::RwLock};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use support::ScratchDatabase;

const ETHEREUM: &str = "ethereum-mainnet";
const REVERSE_RESOLVER: &str = "0xa2c122be93b0074270ebee7f6b7292c7deb45047";
const ADDRESS: &str = "0x00000000000000000000000000000000000000a1";
const ADDRESS_TWO: &str = "0x00000000000000000000000000000000000000a2";
const REVERSE_NODE: &str = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const REVERSE_NODE_TWO: &str = "0x1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const NAMEHASH: &str = "0xabcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
const FAILED_MULTICALL: &str = "__fixture_multicall_failed__";
const FAILED_MULTICALL_BATCH: &str = "__fixture_multicall_batch_failed__";
const MULTICALL_RESULTS_PREFIX: &str = "__fixture_multicall_results__:";

struct FailingInterpretPhase;

impl Phase for FailingInterpretPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Interpret
    }

    fn run_batch(&self, _context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async { Err(RunnerError::data_integrity("forced required-redo failure")) })
    }
}

struct FailingVerifyPreflightPhase;

impl Phase for FailingVerifyPreflightPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Verify
    }

    fn preflight(
        &self,
        _chain_id: &str,
        _sources: &[SourceConfig],
        _mode: &RunMode,
    ) -> phase_runner::error::RunnerResult<()> {
        Err(RunnerError::new(
            phase_runner::error::ErrorKind::Configuration,
            "fixture verify preflight failed",
        ))
    }

    fn run_batch(&self, _context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async { panic!("failed verify preflight must prevent phase execution") })
    }
}

#[tokio::test]
async fn live_head_walk_advances_published_markers() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_head_walk").await?;
    let chain = "live-head-walk";
    seed_branch(scratch.pool(), chain, 1, 0, None).await?;
    publish(scratch.pool(), chain, 1, 0, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 0, &block_hash(1, 0)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    let fixture = RpcFixture::spawn(1, 4).await?;

    let ingest_engine = Arc::new(Engine::new(scratch.pool().clone()));
    let phases = PhaseSet::with_ingest_interpret_project_and_live(
        Arc::new(IngestPhase::with_engine(Arc::clone(&ingest_engine))),
        Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(LoopbackPhase::new(PhaseName::Verify)),
        Arc::new(LivePhase::with_engine(ingest_engine)),
    )?;
    let runner = Arc::new(PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-live-head-walk",
        TimingConfig {
            initial_backoff: Duration::from_millis(1),
            maximum_backoff: Duration::from_millis(4),
            live_poll_interval: Duration::from_millis(1),
        },
    )?);
    let configured_chain = ChainConfig::new(
        chain,
        vec![SourceConfig::new(
            chain,
            "rpc",
            "rpc",
            SeedBasis::NewSignatureRange,
            0,
            fixture.endpoint.clone(),
        )?],
        false,
    )?;
    let cancellation = CancellationToken::new();
    let run_cancellation = cancellation.clone();
    let mut task =
        tokio::spawn(async move { runner.run_chain(&configured_chain, run_cancellation).await });
    {
        let head_advanced = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let latest: i64 = sqlx::query_scalar(
                    "SELECT latest_block_number FROM chain_heads WHERE chain_id = $1",
                )
                .bind(chain)
                .fetch_one(scratch.pool())
                .await?;
                if latest == 4 {
                    return Result::<()>::Ok(());
                }
                tokio::task::yield_now().await;
            }
        });
        tokio::pin!(head_advanced);
        tokio::select! {
            result = &mut task => {
                let result = result?;
                anyhow::bail!("production live runner stopped before advancing the head: {result:?}");
            }
            result = &mut head_advanced => match result {
                Ok(result) => result?,
                Err(error) => {
                    let states: Vec<(String, String, Option<String>)> = sqlx::query_as(
                        "SELECT phase_name, phase_status, last_error
                         FROM chain_phase_state WHERE chain_id = $1 ORDER BY phase_name",
                    )
                    .bind(chain)
                    .fetch_all(scratch.pool())
                    .await?;
                    cancellation.cancel();
                    task.abort();
                    anyhow::bail!("{error}; phase states: {states:?}");
                }
            },
        }
    }
    cancellation.cancel();
    task.await??;

    let stored: (i64, String) = sqlx::query_as(
        "SELECT latest_block_number, latest_block_hash
         FROM chain_heads WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(stored, (4, block_hash(1, 4)));
    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn live_gap_fill_is_bounded_and_contiguous() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_gap_fill").await?;
    let chain = "live-gap-fill";
    seed_branch(scratch.pool(), chain, 1, 0, None).await?;
    publish(scratch.pool(), chain, 1, 0, 0, 0).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    let fixture = RpcFixture::spawn(1, 600).await?;
    let engine = Engine::new(scratch.pool().clone());
    let request = || live_request(chain, &fixture.endpoint, 0, block_hash(1, 0));

    let first = engine.run_live_batch(request()).await?;
    assert!(!first.caught_up);
    assert_eq!(first.current.number, 256);
    publish_ingest_heads(scratch.pool(), chain, first.heads).await?;
    let second = engine.run_live_batch(request()).await?;
    assert!(!second.caught_up);
    assert_eq!(second.current.number, 512);
    publish_ingest_heads(scratch.pool(), chain, second.heads).await?;
    let third = engine.run_live_batch(request()).await?;
    assert!(third.caught_up);
    assert_eq!(third.current.number, 600);
    publish_ingest_heads(scratch.pool(), chain, third.heads).await?;

    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM chain_lineage
         WHERE chain_id = $1 AND canonicality_state <> 'orphaned'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(count, 601);
    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn live_retries_when_the_provider_reorgs_between_ancestry_and_suffix_reads() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_snapshot_race").await?;
    let chain = "live-snapshot-race";
    seed_branch(scratch.pool(), chain, 1, 3, None).await?;
    publish(scratch.pool(), chain, 1, 3, 0, 0).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    let fixture = RpcFixture::spawn(1, 4).await?;
    fixture.reorg_after_next_number_batch(2, 1, 4).await;
    let engine = Engine::new(scratch.pool().clone());
    let request = || live_request(chain, &fixture.endpoint, 3, block_hash(1, 3));

    let error = engine
        .run_live_batch(request())
        .await
        .expect_err("a suffix disconnected by a provider snapshot change must retry");
    assert_eq!(error.kind(), bigname_ingest::ErrorKind::Transient);

    let recovered = engine.run_live_batch(request()).await?;
    assert!(recovered.caught_up);
    assert_eq!(recovered.current.hash, block_hash(2, 4));
    publish_ingest_heads(scratch.pool(), chain, recovered.heads).await?;
    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn restart_after_an_unpublished_live_window_republishes_and_advances_the_spine() -> Result<()>
{
    let scratch = ScratchDatabase::create("production_live_restart_unpublished_window").await?;
    let chain = "live-restart-unpublished-window";
    seed_branch(scratch.pool(), chain, 1, 1, None).await?;
    publish(scratch.pool(), chain, 1, 1, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 0, &block_hash(1, 0)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    let fixture = RpcFixture::spawn(1, 3).await?;
    let engine = Arc::new(Engine::new(scratch.pool().clone()));

    let unpublished = engine
        .run_live_batch(live_request(chain, &fixture.endpoint, 0, block_hash(1, 0)))
        .await?;
    assert_eq!(unpublished.current.number, 3);
    let staged: Vec<(i64, String)> = sqlx::query_as(
        "SELECT block_number, canonicality_state::text
         FROM chain_lineage
         WHERE chain_id = $1 AND block_number > 1
         ORDER BY block_number",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        staged,
        vec![(2, "observed".to_owned()), (3, "observed".to_owned())]
    );

    let runner = production_runner(&scratch, Arc::clone(&engine), chain)?;
    let configured_chain = live_chain(chain, &fixture.endpoint)?;
    let cancellation = CancellationToken::new();
    let run_cancellation = cancellation.clone();
    let mut task =
        tokio::spawn(async move { runner.run_chain(&configured_chain, run_cancellation).await });
    wait_for_rederived_or_runner_stop(scratch.pool(), chain, 3, &block_hash(1, 3), &mut task)
        .await?;
    cancellation.cancel();
    task.await??;

    let recovered: Vec<(i64, String)> = sqlx::query_as(
        "SELECT block_number, canonicality_state::text
         FROM chain_lineage
         WHERE chain_id = $1 AND block_number > 1
         ORDER BY block_number",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        recovered,
        vec![(2, "canonical".to_owned()), (3, "canonical".to_owned())]
    );
    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn restart_after_an_unpublished_losing_fork_orphans_the_observed_suffix() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_restart_losing_observed").await?;
    let chain = "live-restart-losing-observed";
    seed_branch(scratch.pool(), chain, 1, 1, None).await?;
    publish(scratch.pool(), chain, 1, 1, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 0, &block_hash(1, 0)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    let fixture = RpcFixture::spawn(1, 3).await?;
    let engine = Arc::new(Engine::new(scratch.pool().clone()));

    let unpublished = engine
        .run_live_batch(live_request(chain, &fixture.endpoint, 0, block_hash(1, 0)))
        .await?;
    assert_eq!(unpublished.current.hash, block_hash(1, 3));
    fixture.reorg(2, 1, 3).await;

    let runner = production_runner(&scratch, Arc::clone(&engine), chain)?;
    let configured_chain = live_chain(chain, &fixture.endpoint)?;
    let cancellation = CancellationToken::new();
    let run_cancellation = cancellation.clone();
    let mut task =
        tokio::spawn(async move { runner.run_chain(&configured_chain, run_cancellation).await });
    wait_for_rederived_or_runner_stop(scratch.pool(), chain, 3, &block_hash(2, 3), &mut task)
        .await?;
    cancellation.cancel();
    task.await??;

    let suffix: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT block_number, block_hash, canonicality_state::text
         FROM chain_lineage
         WHERE chain_id = $1 AND block_number > 1
         ORDER BY block_number, block_hash",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        suffix,
        vec![
            (2, block_hash(1, 2), "orphaned".to_owned()),
            (2, block_hash(2, 2), "canonical".to_owned()),
            (3, block_hash(1, 3), "orphaned".to_owned()),
            (3, block_hash(2, 3), "canonical".to_owned()),
        ]
    );
    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn live_reorg_walk_loads_the_winning_suffix_and_stamps_cursors() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_reorg_walk").await?;
    let chain = "live-reorg-walk";
    seed_branch(scratch.pool(), chain, 1, 3, None).await?;
    publish(scratch.pool(), chain, 1, 3, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 3, &block_hash(1, 3)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    let fixture = RpcFixture::spawn(1, 3).await?;
    fixture.reorg(2, 1, 3).await;

    let outcome = Engine::new(scratch.pool().clone())
        .run_live_batch(live_request(chain, &fixture.endpoint, 3, block_hash(1, 3)))
        .await?;
    assert!(outcome.caught_up);
    assert_eq!(outcome.current.hash, block_hash(2, 3));
    publish_ingest_heads(scratch.pool(), chain, outcome.heads).await?;

    let lineage: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT block_number, block_hash, canonicality_state::text
         FROM chain_lineage WHERE chain_id = $1 AND block_number >= 2
         ORDER BY block_number, block_hash",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        lineage,
        vec![
            (2, block_hash(1, 2), "orphaned".into()),
            (2, block_hash(2, 2), "canonical".into()),
            (3, block_hash(1, 3), "orphaned".into()),
            (3, block_hash(2, 3), "canonical".into()),
        ]
    );
    let stamps: Vec<(String, Option<i64>, Option<i64>)> = sqlx::query_as(
        "SELECT phase_name, redo_from_block_number, redo_to_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')
         ORDER BY phase_name",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        stamps,
        vec![
            ("interpret".into(), Some(2), Some(3)),
            ("project".into(), Some(2), Some(3)),
        ]
    );
    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn lagging_same_fork_snapshot_does_not_publish_or_stamp() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_lagging_same_fork").await?;
    let chain = "live-lagging-same-fork";
    seed_branch(scratch.pool(), chain, 1, 3, None).await?;
    publish(scratch.pool(), chain, 1, 3, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 3, &block_hash(1, 3)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    let fixture = RpcFixture::spawn(1, 2).await?;

    let outcome = LivePhase::new(scratch.pool().clone())
        .run_batch(live_context(chain, &fixture.endpoint, 3, block_hash(1, 3))?)
        .await?;
    if let Some(heads) = &outcome.progress().heads {
        publish_heads(scratch.pool(), chain, heads).await?;
    }

    assert!(
        outcome.progress().heads.is_none(),
        "a lagging same-fork snapshot must not request head publication"
    );
    let head: (i64, String) = sqlx::query_as(
        "SELECT latest_block_number, latest_block_hash
         FROM chain_heads WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(head, (3, block_hash(1, 3)));
    let suffix_state: String = sqlx::query_scalar(
        "SELECT canonicality_state::text FROM chain_lineage
         WHERE chain_id = $1 AND block_hash = $2",
    )
    .bind(chain)
    .bind(block_hash(1, 3))
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(suffix_state, "canonical");
    let stamps: Vec<(String, bool)> = sqlx::query_as(
        "SELECT phase_name, redo_in_progress
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')
         ORDER BY phase_name",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        stamps,
        vec![
            ("interpret".to_owned(), false),
            ("project".to_owned(), false)
        ]
    );
    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn equal_head_lag_preserves_unpublished_same_fork_observations() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_equal_head_lag").await?;
    let chain = "live-equal-head-lag";
    seed_branch(scratch.pool(), chain, 1, 1, None).await?;
    publish(scratch.pool(), chain, 1, 1, 0, 0).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    let fixture = RpcFixture::spawn(1, 3).await?;
    let engine = Engine::new(scratch.pool().clone());

    let unpublished = engine
        .run_live_batch(live_request(chain, &fixture.endpoint, 1, block_hash(1, 1)))
        .await?;
    assert_eq!(unpublished.current.number, 3);
    fixture.reorg(1, 1, 1).await;
    let lagging = engine
        .run_live_batch(live_request(chain, &fixture.endpoint, 1, block_hash(1, 1)))
        .await?;
    publish_ingest_heads(scratch.pool(), chain, lagging.heads).await?;

    let staged: Vec<(i64, String)> = sqlx::query_as(
        "SELECT block_number, canonicality_state::text
         FROM chain_lineage
         WHERE chain_id = $1 AND block_number > 1
         ORDER BY block_number",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        staged,
        vec![(2, "observed".to_owned()), (3, "observed".to_owned())]
    );
    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn genuine_lower_fork_snapshot_still_publishes_the_reorg() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_genuine_lower_fork").await?;
    let chain = "live-genuine-lower-fork";
    seed_branch(scratch.pool(), chain, 1, 3, None).await?;
    publish(scratch.pool(), chain, 1, 3, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 3, &block_hash(1, 3)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    let fixture = RpcFixture::spawn(1, 3).await?;
    fixture.reorg(2, 1, 2).await;

    let outcome = LivePhase::new(scratch.pool().clone())
        .run_batch(live_context(chain, &fixture.endpoint, 3, block_hash(1, 3))?)
        .await?;
    let heads = outcome
        .progress()
        .heads
        .as_ref()
        .expect("a genuine lower fork must request head publication");
    publish_heads(scratch.pool(), chain, heads).await?;

    let head: (i64, String) = sqlx::query_as(
        "SELECT latest_block_number, latest_block_hash
         FROM chain_heads WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(head, (2, block_hash(2, 2)));
    let stamps: Vec<(String, Option<i64>, Option<i64>)> = sqlx::query_as(
        "SELECT phase_name, redo_from_block_number, redo_to_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')
         ORDER BY phase_name",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        stamps,
        vec![
            ("interpret".to_owned(), Some(2), Some(3)),
            ("project".to_owned(), Some(2), Some(3)),
        ]
    );
    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn live_lower_head_waits_for_the_stamped_range_before_downstream_redo() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_lower_head_reorg").await?;
    let chain = "live-lower-head-reorg";
    seed_branch(scratch.pool(), chain, 1, 3, None).await?;
    publish(scratch.pool(), chain, 1, 3, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 3, &block_hash(1, 3)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    let fixture = RpcFixture::spawn(1, 3).await?;
    fixture.reorg(2, 1, 2).await;

    let ingest_engine = Arc::new(Engine::new(scratch.pool().clone()));
    let phases = PhaseSet::with_ingest_interpret_project_and_live(
        Arc::new(IngestPhase::with_engine(Arc::clone(&ingest_engine))),
        Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(LoopbackPhase::new(PhaseName::Verify)),
        Arc::new(LivePhase::with_engine(ingest_engine)),
    )?;
    let runner = Arc::new(PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-live-lower-head-reorg",
        fast_timing(),
    )?);
    let configured_chain = live_chain(chain, &fixture.endpoint)?;
    let cancellation = CancellationToken::new();
    let run_cancellation = cancellation.clone();
    let mut task =
        tokio::spawn(async move { runner.run_chain(&configured_chain, run_cancellation).await });

    wait_for_head(scratch.pool(), chain, 2, &block_hash(2, 2)).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    if task.is_finished() {
        let result = (&mut task).await?;
        fixture.server.abort();
        scratch.cleanup().await?;
        anyhow::bail!(
            "live runner stopped instead of waiting for the stamped redo range: {result:?}"
        );
    }

    fixture.reorg(2, 1, 3).await;
    wait_for_rederived_head(scratch.pool(), chain, 3, &block_hash(2, 3)).await?;
    cancellation.cancel();
    task.await??;

    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn restart_recovers_live_running_after_head_publication() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_restart_after_publication").await?;
    let chain = "live-restart-after-publication";
    seed_branch(scratch.pool(), chain, 1, 1, None).await?;
    publish(scratch.pool(), chain, 1, 0, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 0, &block_hash(1, 0)).await?;
    publish(scratch.pool(), chain, 1, 1, 0, 0).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running', current_block_number = 1,
             current_block_hash = $2, target_block_number = 1,
             target_block_hash = $2, started_at = now(), finished_at = NULL
         WHERE chain_id = $1 AND phase_name = 'live'",
    )
    .bind(chain)
    .bind(block_hash(1, 1))
    .execute(scratch.pool())
    .await?;

    let runner = Arc::new(PhaseRunner::new(
        scratch.runner(),
        PhaseSet::loopback(),
        CapacityGuard::system(CapacityConfig::default()),
        "production-live-restart-after-publication",
        fast_timing(),
    )?);
    let configured_chain = live_chain(chain, "http://unused.invalid")?;
    let cancellation = CancellationToken::new();
    let run_cancellation = cancellation.clone();
    let task =
        tokio::spawn(async move { runner.run_chain(&configured_chain, run_cancellation).await });
    wait_for_rederived_head(scratch.pool(), chain, 1, &block_hash(1, 1)).await?;
    cancellation.cancel();
    task.await??;

    let cursors: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT phase_name, current_block_hash FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')
         ORDER BY phase_name",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert!(
        cursors
            .iter()
            .all(|(_, hash)| hash.as_deref() == Some(block_hash(1, 1).as_str()))
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn failed_system_redo_keeps_automatic_ownership() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_required_redo_failure").await?;
    let chain = "live-required-redo-failure";
    seed_branch(scratch.pool(), chain, 1, 3, None).await?;
    publish(scratch.pool(), chain, 1, 3, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 3, &block_hash(1, 3)).await?;
    seed_branch(scratch.pool(), chain, 2, 3, Some((1, block_hash(1, 1)))).await?;
    publish(scratch.pool(), chain, 2, 3, 0, 0).await?;

    let phases = PhaseSet::new([
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(FailingInterpretPhase),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(LoopbackPhase::new(PhaseName::Verify)),
        Arc::new(LoopbackPhase::new(PhaseName::Live)),
    ])?;
    let error = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-live-required-redo-failure",
        fast_timing(),
    )?
    .run_chain(
        &live_chain(chain, "http://unused.invalid")?,
        CancellationToken::new(),
    )
    .await
    .expect_err("fixture interpret redo must fail");
    assert!(error.to_string().contains("forced required-redo failure"));

    let state: (bool, Option<String>) = sqlx::query_as(
        "SELECT redo_in_progress, last_error FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert!(state.0);
    assert!(
        state
            .1
            .as_deref()
            .is_some_and(|message| message.starts_with("required downstream redo: "))
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn pending_required_redo_stamp_widens_and_keeps_ownership() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_pending_stamp_widening").await?;
    let chain = "live-pending-stamp-widening";
    seed_branch(scratch.pool(), chain, 1, 5, None).await?;
    publish(scratch.pool(), chain, 1, 5, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 5, &block_hash(1, 5)).await?;

    seed_branch(scratch.pool(), chain, 2, 5, Some((2, block_hash(1, 2)))).await?;
    publish(scratch.pool(), chain, 2, 5, 0, 0).await?;
    let pending: Vec<RedoStampSnapshot> = sqlx::query_as(
        "SELECT phase_name, redo_from_block_number, redo_to_block_number, last_error
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')
         ORDER BY phase_name",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert!(pending.iter().all(|(_, from, to, owner)| {
        *from == Some(3)
            && *to == Some(5)
            && owner
                .as_deref()
                .is_some_and(|message| message.starts_with("required downstream redo: "))
    }));

    seed_branch(scratch.pool(), chain, 3, 5, Some((0, block_hash(1, 0)))).await?;
    publish(scratch.pool(), chain, 3, 5, 0, 0).await?;
    let widened: Vec<RedoStampSnapshot> = sqlx::query_as(
        "SELECT phase_name, redo_from_block_number, redo_to_block_number, last_error
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')
         ORDER BY phase_name",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert!(widened.iter().all(|(_, from, to, owner)| {
        *from == Some(1)
            && *to == Some(5)
            && owner
                .as_deref()
                .is_some_and(|message| message.starts_with("required downstream redo: "))
    }));
    scratch.cleanup().await
}

#[tokio::test]
async fn failed_operator_redo_range_widens_without_losing_the_operator_error() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_operator_stamp_widening").await?;
    let chain = "live-operator-stamp-widening";
    seed_branch(scratch.pool(), chain, 1, 5, None).await?;
    publish(scratch.pool(), chain, 1, 5, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 5, &block_hash(1, 5)).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running', redo_in_progress = true, redo_mode = 'redo',
             redo_previous_phase_status = 'completed',
             redo_previous_started_at = started_at,
             redo_previous_finished_at = finished_at,
             redo_from_block_number = 3, redo_to_block_number = 5,
             redo_current_block_number = 4, redo_current_block_hash = $2,
             redo_target_block_number = 5, redo_target_block_hash = $3,
             last_error = 'operator redo failed: fixture',
             started_at = now(), finished_at = NULL
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .bind(block_hash(1, 4))
    .bind(block_hash(1, 5))
    .execute(scratch.pool())
    .await?;

    seed_branch(scratch.pool(), chain, 2, 5, Some((0, block_hash(1, 0)))).await?;
    publish(scratch.pool(), chain, 2, 5, 0, 0).await?;
    let state: (Option<i64>, Option<i64>, Option<i64>, Option<String>) = sqlx::query_as(
        "SELECT redo_from_block_number, redo_to_block_number,
                redo_current_block_number, last_error
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        state,
        (
            Some(1),
            Some(5),
            None,
            Some("operator redo failed: fixture".to_owned())
        )
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn active_required_redo_blocks_another_writer_phase() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_required_redo_exclusion").await?;
    let chain = "live-required-redo-exclusion";
    seed_branch(scratch.pool(), chain, 1, 2, None).await?;
    publish(scratch.pool(), chain, 1, 2, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 1, &block_hash(1, 1)).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running', redo_in_progress = true, redo_mode = 'redo',
             redo_previous_phase_status = 'completed',
             redo_previous_started_at = started_at,
             redo_previous_finished_at = finished_at,
             redo_from_block_number = 1,
             redo_to_block_number = 1,
             last_error = 'required downstream redo active: active fixture',
             started_at = now(), finished_at = NULL
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;

    let store = PhaseStore::new(scratch.pool().clone());
    let error = store
        .start_phase(chain, PhaseName::Interpret, &RunMode::Normal)
        .await
        .expect_err("an active required project redo must exclude interpret writes");
    assert_eq!(
        error.kind(),
        phase_runner::error::ErrorKind::InvalidTransition
    );
    assert!(error.to_string().contains("while phase project is running"));
    scratch.cleanup().await
}

#[tokio::test]
async fn unsupported_live_redo_is_rejected_without_persisting_redo_state() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_unsupported_redo").await?;
    let chain = "live-unsupported-redo";
    seed_branch(scratch.pool(), chain, 1, 1, None).await?;
    publish(scratch.pool(), chain, 1, 1, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 1, &block_hash(1, 1)).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed', current_block_number = 1,
             current_block_hash = $2, target_block_number = 1,
             target_block_hash = $2, started_at = now(), finished_at = now()
         WHERE chain_id = $1 AND phase_name = 'live'",
    )
    .bind(chain)
    .bind(block_hash(1, 1))
    .execute(scratch.pool())
    .await?;
    let engine = Arc::new(Engine::new(scratch.pool().clone()));
    let runner = PhaseRunner::new(
        scratch.runner(),
        PhaseSet::with_ingest_interpret_project_and_live(
            Arc::new(IngestPhase::with_engine(Arc::clone(&engine))),
            Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
            Arc::new(LoopbackPhase::new(PhaseName::Project)),
            Arc::new(LoopbackPhase::new(PhaseName::Verify)),
            Arc::new(LivePhase::with_engine(engine)),
        )?,
        CapacityGuard::system(CapacityConfig::default()),
        "production-live-unsupported-redo",
        fast_timing(),
    )?;

    let error = runner
        .redo(
            &live_chain(chain, "http://unused.invalid")?,
            RedoPhase::Phase(PhaseName::Live),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("live redo is unavailable");
    assert_eq!(error.kind(), phase_runner::error::ErrorKind::Configuration);
    let state: (String, bool, Option<String>) = sqlx::query_as(
        "SELECT phase_status, redo_in_progress, last_error
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'live'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(state, ("completed".to_owned(), false, None));
    scratch.cleanup().await
}

#[tokio::test]
async fn verify_preflight_failure_is_rejected_before_persisting_redo_state() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_verify_preflight_redo").await?;
    let chain = "live-verify-preflight-redo";
    seed_branch(scratch.pool(), chain, 1, 1, None).await?;
    publish(scratch.pool(), chain, 1, 1, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 1, &block_hash(1, 1)).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed', verification_level = 'quick_synced',
             current_block_number = 1, current_block_hash = $2,
             target_block_number = 1, target_block_hash = $2,
             started_at = now(), finished_at = now(), updated_at = now()
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(chain)
    .bind(block_hash(1, 1))
    .execute(scratch.pool())
    .await?;
    let before: (
        String,
        Option<String>,
        bool,
        Option<i64>,
        Option<i64>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT phase_status, verification_level, redo_in_progress,
                    redo_from_block_number, redo_to_block_number, last_error
             FROM chain_phase_state
             WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;

    let engine = Arc::new(Engine::new(scratch.pool().clone()));
    let runner = PhaseRunner::new(
        scratch.runner(),
        PhaseSet::with_ingest_interpret_project_and_live(
            Arc::new(IngestPhase::with_engine(Arc::clone(&engine))),
            Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
            Arc::new(LoopbackPhase::new(PhaseName::Project)),
            Arc::new(FailingVerifyPreflightPhase),
            Arc::new(LivePhase::with_engine(engine)),
        )?,
        CapacityGuard::system(CapacityConfig::default()),
        "production-live-verify-preflight-redo",
        fast_timing(),
    )?;
    let error = runner
        .redo(
            &live_chain(chain, "http://unused.invalid")?,
            RedoPhase::Phase(PhaseName::Verify),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("verify preflight must refuse redo before creating its marker");
    assert_eq!(error.kind(), phase_runner::error::ErrorKind::Configuration);
    assert!(
        error
            .to_string()
            .contains("fixture verify preflight failed")
    );

    let after: (
        String,
        Option<String>,
        bool,
        Option<i64>,
        Option<i64>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT phase_status, verification_level, redo_in_progress,
                    redo_from_block_number, redo_to_block_number, last_error
             FROM chain_phase_state
             WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(after, before);
    scratch.cleanup().await
}

#[tokio::test]
async fn rewind_uses_head_publication_and_stamps_downstream_redo() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_rewind").await?;
    let chain = "live-rewind";
    seed_branch(scratch.pool(), chain, 1, 3, None).await?;
    publish(scratch.pool(), chain, 1, 3, 1, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 3, &block_hash(1, 3)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;

    let outcome = rewind_to_ancestor(
        &scratch.runner(),
        chain,
        BlockMarker::new(1, block_hash(1, 1))?,
    )
    .await?;
    assert_eq!(outcome.previous.number, 3);
    assert_eq!(outcome.ancestor.number, 1);

    let head: (i64, String) = sqlx::query_as(
        "SELECT latest_block_number, latest_block_hash
         FROM chain_heads WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(head, (1, block_hash(1, 1)));
    let states: Vec<(String, bool, Option<i64>, Option<i64>)> = sqlx::query_as(
        "SELECT phase_name, redo_in_progress,
                redo_from_block_number, redo_to_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')
         ORDER BY phase_name",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        states,
        vec![
            ("interpret".into(), true, Some(2), Some(3)),
            ("project".into(), true, Some(2), Some(3)),
        ]
    );
    let orphaned: Vec<i64> = sqlx::query_scalar(
        "SELECT block_number FROM chain_lineage
         WHERE chain_id = $1 AND canonicality_state = 'orphaned'
         ORDER BY block_number",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(orphaned, vec![2, 3]);
    let fixture = RpcFixture::spawn(1, 3).await?;
    let refilled = Engine::new(scratch.pool().clone())
        .run_live_batch(live_request(chain, &fixture.endpoint, 3, block_hash(1, 3)))
        .await?;
    assert!(refilled.caught_up);
    assert_eq!(refilled.current.hash, block_hash(1, 3));
    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn rewind_refuses_missing_and_non_readable_ancestors_without_state_change() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_rewind_invalid_ancestor").await?;
    let chain = "live-rewind-invalid-ancestor";
    seed_branch(scratch.pool(), chain, 1, 3, None).await?;
    publish(scratch.pool(), chain, 1, 3, 1, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 3, &block_hash(1, 3)).await?;
    seed_branch(scratch.pool(), chain, 2, 2, Some((0, block_hash(1, 0)))).await?;
    let before = rewind_snapshot(scratch.pool(), chain).await?;

    let missing = rewind_to_ancestor(
        &scratch.runner(),
        chain,
        BlockMarker::new(
            1,
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        )?,
    )
    .await
    .expect_err("rewind must refuse an ancestor that is not stored");
    assert_eq!(
        missing.kind(),
        phase_runner::error::ErrorKind::DataIntegrity
    );
    assert!(missing.to_string().contains("is not stored"));
    assert_eq!(rewind_snapshot(scratch.pool(), chain).await?, before);

    let observed = rewind_to_ancestor(
        &scratch.runner(),
        chain,
        BlockMarker::new(1, block_hash(2, 1))?,
    )
    .await
    .expect_err("rewind must refuse an observed ancestor");
    assert_eq!(
        observed.kind(),
        phase_runner::error::ErrorKind::DataIntegrity
    );
    assert!(observed.to_string().contains("not on the readable path"));
    assert_eq!(rewind_snapshot(scratch.pool(), chain).await?, before);
    scratch.cleanup().await
}

#[tokio::test]
async fn rewind_refuses_to_cross_the_safe_head_without_state_change() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_rewind_safe_boundary").await?;
    let chain = "live-rewind-safe-boundary";
    seed_branch(scratch.pool(), chain, 1, 3, None).await?;
    publish(scratch.pool(), chain, 1, 3, 2, 1).await?;
    seed_completed_spine(scratch.pool(), chain, 3, &block_hash(1, 3)).await?;
    let before = rewind_snapshot(scratch.pool(), chain).await?;

    let error = rewind_to_ancestor(
        &scratch.runner(),
        chain,
        BlockMarker::new(1, block_hash(1, 1))?,
    )
    .await
    .expect_err("rewind must not cross the published safe head");
    assert_eq!(error.kind(), phase_runner::error::ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("below safe block 2"));
    assert_eq!(rewind_snapshot(scratch.pool(), chain).await?, before);
    scratch.cleanup().await
}

#[tokio::test]
async fn rewind_refuses_to_overlap_a_downstream_writer() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_rewind_lock").await?;
    let chain = "live-rewind-lock";
    seed_branch(scratch.pool(), chain, 1, 2, None).await?;
    publish(scratch.pool(), chain, 1, 2, 0, 0).await?;
    let database = scratch.runner();
    let mut project_lock = scratch.pool().acquire().await?;
    let lock_name = format!("phase-runner:{chain}:project");
    sqlx::query("SELECT pg_advisory_lock(hashtextextended($1::text, 0::bigint))")
        .bind(&lock_name)
        .execute(&mut *project_lock)
        .await?;

    let error = rewind_to_ancestor(&database, chain, BlockMarker::new(1, block_hash(1, 1))?)
        .await
        .expect_err("rewind must not race an active downstream writer");
    assert_eq!(error.kind(), phase_runner::error::ErrorKind::LockHeld);

    let released: bool =
        sqlx::query_scalar("SELECT pg_advisory_unlock(hashtextextended($1::text, 0::bigint))")
            .bind(lock_name)
            .fetch_one(&mut *project_lock)
            .await?;
    assert!(released);
    scratch.cleanup().await
}

#[tokio::test]
async fn event_silent_reverse_hydration_refreshes_and_follows_a_fork() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_reverse_hydration").await?;
    seed_branch(scratch.pool(), ETHEREUM, 1, 2, None).await?;
    publish(scratch.pool(), ETHEREUM, 1, 1, 0, 0).await?;
    seed_reverse_candidate(scratch.pool()).await?;
    let rpc = HydrationRpc::spawn(BTreeMap::from([
        (block_hash(1, 1), "alice.eth".to_owned()),
        (block_hash(1, 2), "bob.eth".to_owned()),
        (block_hash(2, 2), String::new()),
    ]))
    .await?;
    let hydrator = Hydrator::new(
        scratch.pool().clone(),
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={}", rpc.endpoint)])?,
    );

    let first = hydrator.hydrate_canonical_head(ETHEREUM).await?;
    assert_eq!(first.updated_rows, 1);
    assert_primary(
        scratch.pool(),
        "success",
        Some("alice.eth"),
        &block_hash(1, 1),
    )
    .await?;

    publish(scratch.pool(), ETHEREUM, 1, 2, 0, 0).await?;
    hydrator.hydrate_canonical_head(ETHEREUM).await?;
    assert_primary(
        scratch.pool(),
        "success",
        Some("bob.eth"),
        &block_hash(1, 2),
    )
    .await?;

    seed_branch(scratch.pool(), ETHEREUM, 2, 2, Some((1, block_hash(1, 1)))).await?;
    publish(scratch.pool(), ETHEREUM, 2, 2, 0, 0).await?;
    hydrator.hydrate_canonical_head(ETHEREUM).await?;
    assert_primary(scratch.pool(), "not_found", None, &block_hash(2, 2)).await?;

    rpc.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn missing_hydration_rpc_fails_before_retracting_existing_values() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_missing_hydration_rpc").await?;
    seed_branch(scratch.pool(), ETHEREUM, 1, 1, None).await?;
    publish(scratch.pool(), ETHEREUM, 1, 1, 0, 0).await?;
    seed_reverse_candidate(scratch.pool()).await?;
    let resource = seed_text_candidate(scratch.pool()).await?;
    let rpc =
        HydrationRpc::spawn(BTreeMap::from([(block_hash(1, 1), "alice.eth".to_owned())])).await?;
    Hydrator::new(
        scratch.pool().clone(),
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={}", rpc.endpoint)])?,
    )
    .hydrate_canonical_head(ETHEREUM)
    .await?;
    let primary_before: (String, Option<String>, Value) = sqlx::query_as(
        "SELECT claim_status, raw_claim_name, claim_provenance
         FROM primary_names_current
         WHERE address = $1 AND coin_type = '60' AND namespace = 'ens'",
    )
    .bind(ADDRESS)
    .fetch_one(scratch.pool())
    .await?;
    let text_before = text_entry(scratch.pool(), resource).await?;

    let error = ProjectPhase::with_hydration(scratch.pool().clone(), ChainRpcUrls::default())
        .run_batch(PhaseContext {
            chain_id: ETHEREUM.to_owned(),
            phase: PhaseName::Project,
            mode: RunMode::Normal,
            sources: Arc::from([]),
            available_heads: Some(HeadMarkers {
                latest: BlockMarker::new(1, block_hash(1, 1))?,
                safe: Some(BlockMarker::new(0, block_hash(1, 0))?),
                finalized: Some(BlockMarker::new(0, block_hash(1, 0))?),
            }),
            live_handoff: None,
            resume: PhaseResume::default(),
        })
        .await
        .expect_err("hydration candidates require a configured RPC URL");
    assert_eq!(error.kind(), phase_runner::error::ErrorKind::Configuration);
    let primary_after: (String, Option<String>, Value) = sqlx::query_as(
        "SELECT claim_status, raw_claim_name, claim_provenance
         FROM primary_names_current
         WHERE address = $1 AND coin_type = '60' AND namespace = 'ens'",
    )
    .bind(ADDRESS)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(primary_after, primary_before);
    assert_eq!(text_entry(scratch.pool(), resource).await?, text_before);

    rpc.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn project_redo_behind_the_canonical_head_defers_hydration() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_redo_hydration_boundary").await?;
    seed_branch(scratch.pool(), ETHEREUM, 1, 2, None).await?;
    publish(scratch.pool(), ETHEREUM, 1, 2, 0, 0).await?;
    seed_completed_spine(scratch.pool(), ETHEREUM, 1, &block_hash(1, 1)).await?;
    seed_reverse_candidate(scratch.pool()).await?;
    let rpc = HydrationRpc::spawn(BTreeMap::from([(
        block_hash(1, 2),
        "future-head.eth".to_owned(),
    )]))
    .await?;
    let phase = ProjectPhase::with_hydration(
        scratch.pool().clone(),
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={}", rpc.endpoint)])?,
    );

    phase
        .run_batch(PhaseContext {
            chain_id: ETHEREUM.to_owned(),
            phase: PhaseName::Project,
            mode: RunMode::Redo(BlockRange::new(1, 1)?),
            sources: Arc::from([]),
            available_heads: Some(HeadMarkers {
                latest: BlockMarker::new(1, block_hash(1, 1))?,
                safe: None,
                finalized: None,
            }),
            live_handoff: None,
            resume: PhaseResume::default(),
        })
        .await?;

    let row: (String, Option<String>, Option<Value>) = sqlx::query_as(
        "SELECT claim_status, raw_claim_name,
                claim_provenance -> 'canonical_head_multicall_hydration'
         FROM primary_names_current
         WHERE address = $1 AND coin_type = '60' AND namespace = 'ens'",
    )
    .bind(ADDRESS)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(row, ("not_found".to_owned(), None, None));
    rpc.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn failed_reverse_hydration_retracts_the_previous_head_value() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_reverse_hydration_failure").await?;
    seed_branch(scratch.pool(), ETHEREUM, 1, 2, None).await?;
    publish(scratch.pool(), ETHEREUM, 1, 1, 0, 0).await?;
    seed_reverse_candidate(scratch.pool()).await?;
    let rpc = HydrationRpc::spawn(BTreeMap::from([
        (block_hash(1, 1), "alice.eth".to_owned()),
        (block_hash(2, 2), FAILED_MULTICALL.to_owned()),
    ]))
    .await?;
    let hydrator = Hydrator::new(
        scratch.pool().clone(),
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={}", rpc.endpoint)])?,
    );

    hydrator.hydrate_canonical_head(ETHEREUM).await?;
    seed_branch(scratch.pool(), ETHEREUM, 2, 2, Some((1, block_hash(1, 1)))).await?;
    publish(scratch.pool(), ETHEREUM, 2, 2, 0, 0).await?;
    let error = hydrator
        .hydrate_canonical_head(ETHEREUM)
        .await
        .expect_err("failed fork hydration must remain retryable");
    assert_eq!(error.kind(), bigname_project::ErrorKind::Transient);

    let row: (String, Option<String>, Option<Value>) = sqlx::query_as(
        "SELECT claim_status, raw_claim_name,
                claim_provenance -> 'canonical_head_multicall_hydration'
         FROM primary_names_current
         WHERE address = $1 AND coin_type = '60' AND namespace = 'ens'",
    )
    .bind(ADDRESS)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(row, ("unsupported".to_owned(), None, None));

    rpc.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn reverse_hydration_rpc_failure_retracts_the_previous_head_value() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_reverse_hydration_rpc_failure").await?;
    seed_branch(scratch.pool(), ETHEREUM, 1, 2, None).await?;
    publish(scratch.pool(), ETHEREUM, 1, 1, 0, 0).await?;
    seed_reverse_candidate(scratch.pool()).await?;
    let rpc = HydrationRpc::spawn(BTreeMap::from([
        (block_hash(1, 1), "alice.eth".to_owned()),
        (block_hash(2, 2), FAILED_MULTICALL_BATCH.to_owned()),
    ]))
    .await?;
    let hydrator = Hydrator::new(
        scratch.pool().clone(),
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={}", rpc.endpoint)])?,
    );

    hydrator.hydrate_canonical_head(ETHEREUM).await?;
    seed_branch(scratch.pool(), ETHEREUM, 2, 2, Some((1, block_hash(1, 1)))).await?;
    publish(scratch.pool(), ETHEREUM, 2, 2, 0, 0).await?;
    let error = hydrator
        .hydrate_canonical_head(ETHEREUM)
        .await
        .expect_err("RPC failure must retract the prior fork and remain retryable");
    assert_eq!(error.kind(), bigname_project::ErrorKind::Transient);

    let row: (String, Option<String>, Option<Value>) = sqlx::query_as(
        "SELECT claim_status, raw_claim_name,
                claim_provenance -> 'canonical_head_multicall_hydration'
         FROM primary_names_current
         WHERE address = $1 AND coin_type = '60' AND namespace = 'ens'",
    )
    .bind(ADDRESS)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(row, ("unsupported".to_owned(), None, None));

    rpc.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn reverse_hydration_retracts_when_the_legacy_resolver_becomes_ineligible() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_reverse_hydration_ineligible").await?;
    seed_branch(scratch.pool(), ETHEREUM, 1, 2, None).await?;
    publish(scratch.pool(), ETHEREUM, 1, 1, 0, 0).await?;
    seed_reverse_candidate(scratch.pool()).await?;
    let rpc =
        HydrationRpc::spawn(BTreeMap::from([(block_hash(1, 1), "alice.eth".to_owned())])).await?;
    let hydrator = Hydrator::new(
        scratch.pool().clone(),
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={}", rpc.endpoint)])?,
    );

    hydrator.hydrate_canonical_head(ETHEREUM).await?;
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash,
             derivation_kind, canonicality_state, after_state
         ) VALUES (
             'reverse-resolver-ineligible', 'ens', 'ResolverChanged',
             'ens_v1_reverse_l1', 1, $1, 2, $2,
             'ens_v1_unwrapped_authority', 'canonical', $3
         )",
    )
    .bind(ETHEREUM)
    .bind(block_hash(1, 2))
    .bind(json!({
        "node": REVERSE_NODE,
        "resolver": "0x0000000000000000000000000000000000000000"
    }))
    .execute(scratch.pool())
    .await?;
    publish(scratch.pool(), ETHEREUM, 1, 2, 0, 0).await?;
    let outcome = hydrator.hydrate_canonical_head(ETHEREUM).await?;
    assert_eq!(outcome.updated_rows, 1);

    let row: (String, Option<String>, Option<Value>) = sqlx::query_as(
        "SELECT claim_status, raw_claim_name,
                claim_provenance -> 'canonical_head_multicall_hydration'
         FROM primary_names_current
         WHERE address = $1 AND coin_type = '60' AND namespace = 'ens'",
    )
    .bind(ADDRESS)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(row, ("unsupported".to_owned(), None, None));

    rpc.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn shortened_reverse_multicall_retracts_every_candidate_baseline() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_reverse_hydration_cardinality").await?;
    seed_branch(scratch.pool(), ETHEREUM, 1, 2, None).await?;
    publish(scratch.pool(), ETHEREUM, 1, 1, 0, 0).await?;
    seed_reverse_candidate(scratch.pool()).await?;
    seed_reverse_candidate_for(scratch.pool(), ADDRESS_TWO, REVERSE_NODE_TWO, "-two").await?;
    let rpc = HydrationRpc::spawn(BTreeMap::from([
        (
            block_hash(1, 1),
            format!("{MULTICALL_RESULTS_PREFIX}alice.eth|bob.eth"),
        ),
        (block_hash(1, 2), "winning.eth".to_owned()),
    ]))
    .await?;
    let hydrator = Hydrator::new(
        scratch.pool().clone(),
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={}", rpc.endpoint)])?,
    );

    hydrator.hydrate_canonical_head(ETHEREUM).await?;
    publish(scratch.pool(), ETHEREUM, 1, 2, 0, 0).await?;
    let error = hydrator
        .hydrate_canonical_head(ETHEREUM)
        .await
        .expect_err("a shortened reverse result must remain retryable");
    assert_eq!(error.kind(), bigname_project::ErrorKind::Transient);

    let rows: Vec<(String, String, Option<String>, Option<Value>)> = sqlx::query_as(
        "SELECT address, claim_status, raw_claim_name,
                claim_provenance -> 'canonical_head_multicall_hydration'
         FROM primary_names_current ORDER BY address",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        rows,
        vec![
            (ADDRESS.to_owned(), "unsupported".to_owned(), None, None),
            (ADDRESS_TWO.to_owned(), "unsupported".to_owned(), None, None,),
        ]
    );

    rpc.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn valueless_legacy_text_hydration_is_head_pinned_and_refreshes() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_text_hydration").await?;
    seed_branch(scratch.pool(), ETHEREUM, 1, 2, None).await?;
    publish(scratch.pool(), ETHEREUM, 1, 1, 0, 0).await?;
    let resource = seed_text_candidate(scratch.pool()).await?;
    let rpc = HydrationRpc::spawn(BTreeMap::from([
        (block_hash(1, 1), "https://one.test".to_owned()),
        (block_hash(1, 2), String::new()),
    ]))
    .await?;
    let hydrator = Hydrator::new(
        scratch.pool().clone(),
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={}", rpc.endpoint)])?,
    );

    hydrator.hydrate_canonical_head(ETHEREUM).await?;
    let first = text_entry(scratch.pool(), resource).await?;
    assert_eq!(first["status"], "success");
    assert_eq!(first["value"], "https://one.test");
    assert_eq!(
        first["canonical_head_multicall_hydration"]["block_hash"],
        block_hash(1, 1)
    );

    publish(scratch.pool(), ETHEREUM, 1, 2, 0, 0).await?;
    hydrator.hydrate_canonical_head(ETHEREUM).await?;
    let refreshed = text_entry(scratch.pool(), resource).await?;
    assert_eq!(refreshed["status"], "not_found");
    assert!(refreshed.get("value").is_none());
    assert_eq!(
        refreshed["canonical_head_multicall_hydration"]["block_hash"],
        block_hash(1, 2)
    );

    rpc.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn failed_text_hydration_retracts_the_previous_head_value() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_text_hydration_failure").await?;
    seed_branch(scratch.pool(), ETHEREUM, 1, 2, None).await?;
    publish(scratch.pool(), ETHEREUM, 1, 1, 0, 0).await?;
    let resource = seed_text_candidate(scratch.pool()).await?;
    let rpc = HydrationRpc::spawn(BTreeMap::from([
        (block_hash(1, 1), "https://one.test".to_owned()),
        (block_hash(2, 2), FAILED_MULTICALL.to_owned()),
    ]))
    .await?;
    let hydrator = Hydrator::new(
        scratch.pool().clone(),
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={}", rpc.endpoint)])?,
    );

    hydrator.hydrate_canonical_head(ETHEREUM).await?;
    seed_branch(scratch.pool(), ETHEREUM, 2, 2, Some((1, block_hash(1, 1)))).await?;
    publish(scratch.pool(), ETHEREUM, 2, 2, 0, 0).await?;
    let error = hydrator
        .hydrate_canonical_head(ETHEREUM)
        .await
        .expect_err("failed fork hydration must remain retryable");
    assert_eq!(error.kind(), bigname_project::ErrorKind::Transient);

    let entry = text_entry(scratch.pool(), resource).await?;
    assert_eq!(entry["status"], "unsupported");
    assert_eq!(
        entry["unsupported_reason"],
        "value_not_retained_in_normalized_events"
    );
    assert!(entry.get("value").is_none());
    assert!(entry.get("canonical_head_multicall_hydration").is_none());

    rpc.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn text_hydration_rpc_failure_retracts_the_previous_head_value() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_text_hydration_rpc_failure").await?;
    seed_branch(scratch.pool(), ETHEREUM, 1, 2, None).await?;
    publish(scratch.pool(), ETHEREUM, 1, 1, 0, 0).await?;
    let resource = seed_text_candidate(scratch.pool()).await?;
    let rpc = HydrationRpc::spawn(BTreeMap::from([
        (block_hash(1, 1), "https://one.test".to_owned()),
        (block_hash(2, 2), FAILED_MULTICALL_BATCH.to_owned()),
    ]))
    .await?;
    let hydrator = Hydrator::new(
        scratch.pool().clone(),
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={}", rpc.endpoint)])?,
    );

    hydrator.hydrate_canonical_head(ETHEREUM).await?;
    seed_branch(scratch.pool(), ETHEREUM, 2, 2, Some((1, block_hash(1, 1)))).await?;
    publish(scratch.pool(), ETHEREUM, 2, 2, 0, 0).await?;
    let error = hydrator
        .hydrate_canonical_head(ETHEREUM)
        .await
        .expect_err("RPC failure must retract the prior fork and remain retryable");
    assert_eq!(error.kind(), bigname_project::ErrorKind::Transient);

    let entry = text_entry(scratch.pool(), resource).await?;
    assert_eq!(entry["status"], "unsupported");
    assert_eq!(
        entry["unsupported_reason"],
        "value_not_retained_in_normalized_events"
    );
    assert!(entry.get("value").is_none());
    assert!(entry.get("canonical_head_multicall_hydration").is_none());

    rpc.server.abort();
    scratch.cleanup().await
}

fn live_request(
    chain: &str,
    endpoint: &str,
    handoff: i64,
    handoff_hash: String,
) -> LiveBatchRequest {
    LiveBatchRequest {
        chain_id: chain.to_owned(),
        sources: vec![SourceDescriptor {
            key: "rpc".to_owned(),
            kind: "rpc".to_owned(),
            start_block: 0,
            endpoint: endpoint.to_owned(),
        }],
        live_handoff: Marker {
            number: handoff,
            hash: handoff_hash,
        },
    }
}

async fn publish_ingest_heads(
    pool: &PgPool,
    chain: &str,
    heads: Option<bigname_ingest::HeadMarkers>,
) -> Result<()> {
    let heads = heads.ok_or_else(|| anyhow::anyhow!("live batch did not request publication"))?;
    publish_heads(
        pool,
        chain,
        &HeadMarkers {
            latest: BlockMarker::new(heads.latest.number, heads.latest.hash)?,
            safe: heads
                .safe
                .map(|marker| BlockMarker::new(marker.number, marker.hash))
                .transpose()?,
            finalized: heads
                .finalized
                .map(|marker| BlockMarker::new(marker.number, marker.hash))
                .transpose()?,
        },
    )
    .await?;
    Ok(())
}

async fn publish(
    pool: &PgPool,
    chain: &str,
    branch: u64,
    latest: i64,
    safe: i64,
    finalized: i64,
) -> Result<()> {
    publish_heads(
        pool,
        chain,
        &HeadMarkers {
            latest: BlockMarker::new(latest, block_hash(branch, latest))?,
            safe: Some(BlockMarker::new(safe, block_hash(1, safe))?),
            finalized: Some(BlockMarker::new(finalized, block_hash(1, finalized))?),
        },
    )
    .await?;
    Ok(())
}

async fn seed_branch(
    pool: &PgPool,
    chain: &str,
    branch: u64,
    through: i64,
    fork: Option<(i64, String)>,
) -> Result<()> {
    let start = fork.as_ref().map_or(0, |(number, _)| number + 1);
    for number in start..=through {
        let parent = if number == 0 {
            None
        } else if number == start {
            fork.as_ref()
                .map(|(_, hash)| hash.clone())
                .or_else(|| Some(block_hash(branch, number - 1)))
        } else {
            Some(block_hash(branch, number - 1))
        };
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, $4, to_timestamp($4 + 1), 'observed')",
        )
        .bind(chain)
        .bind(block_hash(branch, number))
        .bind(parent)
        .bind(number)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn seed_completed_spine(pool: &PgPool, chain: &str, number: i64, hash: &str) -> Result<()> {
    let store = PhaseStore::new(pool.clone());
    store.initialize_chain(chain).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed', current_block_number = $2,
             current_block_hash = $3, target_block_number = $2,
             target_block_hash = $3,
             live_handoff_block_number = CASE WHEN phase_name = 'ingest' THEN $2 END,
             live_handoff_block_hash = CASE WHEN phase_name = 'ingest' THEN $3 END,
             input_content_hash = CASE
                 WHEN phase_name IN ('interpret', 'project') THEN $4 END,
             started_at = now(), finished_at = now(), updated_at = now()
         WHERE chain_id = $1 AND phase_name IN ('ingest', 'interpret', 'project')",
    )
    .bind(chain)
    .bind(number)
    .bind(hash)
    .bind(INTERPRETER_CONTENT_HASH)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO ingest_cursors (
             chain_id, source_key, source_kind, seed_basis, start_block_number,
             next_block_number, target_block_number, last_processed_block_number,
             last_processed_block_hash
         ) VALUES ($1, 'rpc', 'rpc', 'new_signature_range', 0,
                   $2, $3, $3, $4)",
    )
    .bind(chain)
    .bind(number.saturating_add(1))
    .bind(number)
    .bind(hash)
    .execute(pool)
    .await?;
    Ok(())
}

fn fast_timing() -> TimingConfig {
    TimingConfig {
        initial_backoff: Duration::from_millis(1),
        maximum_backoff: Duration::from_millis(4),
        live_poll_interval: Duration::from_millis(1),
    }
}

fn live_chain(chain: &str, endpoint: &str) -> phase_runner::error::RunnerResult<ChainConfig> {
    ChainConfig::new(
        chain,
        vec![SourceConfig::new(
            chain,
            "rpc",
            "rpc",
            SeedBasis::NewSignatureRange,
            0,
            endpoint,
        )?],
        false,
    )
}

fn production_runner(
    scratch: &ScratchDatabase,
    engine: Arc<Engine>,
    chain: &str,
) -> Result<Arc<PhaseRunner>> {
    let phases = PhaseSet::with_ingest_interpret_project_and_live(
        Arc::new(IngestPhase::with_engine(Arc::clone(&engine))),
        Arc::new(InterpretPhase::new(scratch.pool().clone())),
        Arc::new(ProjectPhase::new(scratch.pool().clone())),
        Arc::new(LoopbackPhase::new(PhaseName::Verify)),
        Arc::new(LivePhase::with_engine(engine)),
    )?;
    Ok(Arc::new(PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        format!("production-{chain}"),
        fast_timing(),
    )?))
}

fn live_context(
    chain: &str,
    endpoint: &str,
    current: i64,
    current_hash: String,
) -> phase_runner::error::RunnerResult<PhaseContext> {
    let current_marker = BlockMarker::new(current, current_hash)?;
    Ok(PhaseContext {
        chain_id: chain.to_owned(),
        phase: PhaseName::Live,
        mode: RunMode::Normal,
        sources: Arc::from([SourceConfig::new(
            chain,
            "rpc",
            "rpc",
            SeedBasis::NewSignatureRange,
            0,
            endpoint,
        )?]),
        available_heads: Some(HeadMarkers {
            latest: current_marker.clone(),
            safe: Some(BlockMarker::new(0, block_hash(1, 0))?),
            finalized: Some(BlockMarker::new(0, block_hash(1, 0))?),
        }),
        live_handoff: Some(current_marker.clone()),
        resume: PhaseResume {
            current: Some(current_marker.clone()),
            target: Some(current_marker),
            ..PhaseResume::default()
        },
    })
}

async fn wait_for_rederived_or_runner_stop(
    pool: &PgPool,
    chain: &str,
    number: i64,
    hash: &str,
    task: &mut tokio::task::JoinHandle<phase_runner::error::RunnerResult<()>>,
) -> Result<()> {
    let wait = wait_for_rederived_head(pool, chain, number, hash);
    tokio::pin!(wait);
    tokio::select! {
        result = task => match result {
            Ok(Ok(())) => anyhow::bail!("production live runner stopped before recovery completed"),
            Ok(Err(error)) => Err(error.into()),
            Err(error) => Err(error.into()),
        },
        result = &mut wait => result,
    }
}

type RewindSnapshot = (
    Option<(
        i64,
        String,
        Option<i64>,
        Option<String>,
        Option<i64>,
        Option<String>,
    )>,
    Vec<(i64, String, String)>,
    Vec<(
        String,
        String,
        bool,
        Option<i64>,
        Option<i64>,
        Option<String>,
    )>,
);
type RedoStampSnapshot = (String, Option<i64>, Option<i64>, Option<String>);

async fn rewind_snapshot(pool: &PgPool, chain: &str) -> Result<RewindSnapshot> {
    let heads = sqlx::query_as(
        "SELECT latest_block_number, latest_block_hash,
                safe_block_number, safe_block_hash,
                finalized_block_number, finalized_block_hash
         FROM chain_heads WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_optional(pool)
    .await?;
    let lineage = sqlx::query_as(
        "SELECT block_number, block_hash, canonicality_state::text
         FROM chain_lineage WHERE chain_id = $1
         ORDER BY block_number, block_hash",
    )
    .bind(chain)
    .fetch_all(pool)
    .await?;
    let phases = sqlx::query_as(
        "SELECT phase_name, phase_status, redo_in_progress,
                redo_from_block_number, redo_to_block_number, last_error
         FROM chain_phase_state WHERE chain_id = $1
         ORDER BY phase_name",
    )
    .bind(chain)
    .fetch_all(pool)
    .await?;
    Ok((heads, lineage, phases))
}

async fn wait_for_head(pool: &PgPool, chain: &str, number: i64, hash: &str) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current: (i64, String) = sqlx::query_as(
                "SELECT latest_block_number, latest_block_hash
                 FROM chain_heads WHERE chain_id = $1",
            )
            .bind(chain)
            .fetch_one(pool)
            .await?;
            if current == (number, hash.to_owned()) {
                return Result::<()>::Ok(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    Ok(())
}

async fn wait_for_rederived_head(
    pool: &PgPool,
    chain: &str,
    number: i64,
    hash: &str,
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let head: (i64, String) = sqlx::query_as(
                "SELECT latest_block_number, latest_block_hash
                 FROM chain_heads WHERE chain_id = $1",
            )
            .bind(chain)
            .fetch_one(pool)
            .await?;
            let phases: Vec<(String, String, bool, Option<String>)> = sqlx::query_as(
                "SELECT phase_name, phase_status, redo_in_progress, current_block_hash
                 FROM chain_phase_state
                 WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')
                 ORDER BY phase_name",
            )
            .bind(chain)
            .fetch_all(pool)
            .await?;
            if head == (number, hash.to_owned())
                && phases.iter().all(|(_, status, redo, current_hash)| {
                    status == "completed" && !redo && current_hash.as_deref() == Some(hash)
                })
            {
                return Result::<()>::Ok(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    Ok(())
}

async fn seed_empty_watch_manifest(pool: &PgPool, chain: &str) -> Result<()> {
    let address = "0x0000000000000000000000000000000000000001";
    let contract = Uuid::new_v4();
    let payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v1_registry_l1",
        "chain": chain,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": "ensip15@ens-normalize-0.1.1",
        "capability_flags": {},
        "roots": [],
        "contracts": [{
            "role": "registry",
            "address": address,
            "proxy_kind": "none",
            "start_block": 0
        }],
        "discovery_rules": [],
        "abi": {"events": [{
            "name": "NewTTL",
            "fragment": "event NewTTL(bytes32 indexed node, uint64 ttl)",
            "emitter_roles": ["registry"],
            "normalized_events": []
        }], "calls": []}
    });
    sqlx::query(
        "INSERT INTO contract_instances (
             contract_instance_id, chain_id, contract_kind
         ) VALUES ($1, $2, 'contract')",
    )
    .bind(contract)
    .bind(chain)
    .execute(pool)
    .await?;
    let manifest: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (
             manifest_version, namespace, source_family, chain_id,
             deployment_label, rollout_status, normalizer_version,
             file_path, manifest_payload
         ) VALUES (1, 'ens', 'ens_v1_registry_l1', $1, 'fixture', 'active',
                   'ensip15@ens-normalize-0.1.1', $2, $3)
         RETURNING manifest_id",
    )
    .bind(chain)
    .bind(format!("tests/{chain}.toml"))
    .bind(payload)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances (
             manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind,
             start_block_number
         ) VALUES ($1, $2, 'contract', 'registry', $3, $4, 'registry', 'none', 0)",
    )
    .bind(manifest)
    .bind(chain)
    .bind(contract)
    .bind(address)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address, active_from_block_number,
             source_manifest_id
         ) VALUES ($1, $2, $3, 0, $4)",
    )
    .bind(contract)
    .bind(chain)
    .bind(address)
    .bind(manifest)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_reverse_candidate(pool: &PgPool) -> Result<()> {
    seed_reverse_candidate_for(pool, ADDRESS, REVERSE_NODE, "").await
}

async fn seed_reverse_candidate_for(
    pool: &PgPool,
    address: &str,
    reverse_node: &str,
    identity_suffix: &str,
) -> Result<()> {
    for (identity, kind, after_state, derivation) in [
        (
            format!("reverse-claim{identity_suffix}"),
            "ReverseChanged",
            json!({
                "address": address,
                "coin_type": "60",
                "namespace": "ens",
                "reverse_node": reverse_node
            }),
            "ens_v1_reverse_claim",
        ),
        (
            format!("reverse-resolver{identity_suffix}"),
            "ResolverChanged",
            json!({"node": reverse_node, "resolver": REVERSE_RESOLVER}),
            "ens_v1_unwrapped_authority",
        ),
    ] {
        sqlx::query(
            "INSERT INTO normalized_events (
                 event_identity, namespace, event_kind, source_family,
                 manifest_version, chain_id, block_number, block_hash,
                 derivation_kind, canonicality_state, after_state
             ) VALUES ($1, 'ens', $2, 'ens_v1_reverse_l1', 1, $3, 1, $4,
                       $5, 'canonical', $6)",
        )
        .bind(identity)
        .bind(kind)
        .bind(ETHEREUM)
        .bind(block_hash(1, 1))
        .bind(derivation)
        .bind(after_state)
        .execute(pool)
        .await?;
    }
    sqlx::query(
        "INSERT INTO primary_names_current (
             address, coin_type, namespace, claim_status,
             unsupported_reason, claim_provenance
         ) VALUES ($1, '60', 'ens', 'unsupported',
                   'legacy_resolver_does_not_emit_name', '{}'::jsonb)",
    )
    .bind(address)
    .execute(pool)
    .await?;
    Ok(())
}

async fn assert_primary(
    pool: &PgPool,
    status: &str,
    name: Option<&str>,
    head_hash: &str,
) -> Result<()> {
    let row: (String, Option<String>, String) = sqlx::query_as(
        "SELECT claim_status, raw_claim_name,
                claim_provenance -> 'canonical_head_multicall_hydration' ->> 'block_hash'
         FROM primary_names_current
         WHERE address = $1 AND coin_type = '60' AND namespace = 'ens'",
    )
    .bind(ADDRESS)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        row,
        (
            status.to_owned(),
            name.map(str::to_owned),
            head_hash.to_owned()
        )
    );
    Ok(())
}

async fn seed_text_candidate(pool: &PgPool) -> Result<Uuid> {
    let resource = Uuid::new_v4();
    let logical_name_id = format!("ens:{NAMEHASH}");
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 1, 'canonical')",
    )
    .bind(resource)
    .bind(ETHEREUM)
    .bind(block_hash(1, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version,
             visibility_state, chain_id, block_hash, block_number,
             canonicality_state
         ) VALUES ($1, 'ens', 'alice.eth', ARRAY['alice','eth'], '\\x'::bytea,
                   $2, ARRAY['0xalice','0xeth'], 'fixture', 'active',
                   $3, $4, 1, 'canonical')",
    )
    .bind(&logical_name_id)
    .bind(NAMEHASH)
    .bind(ETHEREUM)
    .bind(block_hash(1, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO record_inventory_current (
             resource_id, record_version_boundary_key, entries,
             support_status, provenance, manifest_version
         ) VALUES ($1, 'resolver:fixture', $2, 'supported', $3, 1)",
    )
    .bind(resource)
    .bind(json!([{
        "record_key": "text:url",
        "record_family": "text",
        "selector_key": "url",
        "status": "unsupported",
        "unsupported_reason": "value_not_retained_in_normalized_events"
    }]))
    .bind(json!({
        "chain_id": ETHEREUM,
        "logical_name_id": logical_name_id,
        "resolver_address": REVERSE_RESOLVER
    }))
    .execute(pool)
    .await?;
    Ok(resource)
}

async fn text_entry(pool: &PgPool, resource: Uuid) -> Result<Value> {
    let entries: Value =
        sqlx::query_scalar("SELECT entries FROM record_inventory_current WHERE resource_id = $1")
            .bind(resource)
            .fetch_one(pool)
            .await?;
    Ok(entries
        .as_array()
        .and_then(|entries| entries.first())
        .cloned()
        .expect("fixture text entry"))
}

fn block_hash(branch: u64, number: i64) -> String {
    format!("0x{:064x}", branch * 1_000_000 + number as u64 + 1)
}

#[derive(Clone)]
struct RpcChain {
    canonical: Vec<String>,
    blocks: BTreeMap<String, Value>,
    reorg_after_number_batch: Option<(u64, i64, i64)>,
}

struct RpcFixture {
    endpoint: String,
    state: Arc<RwLock<RpcChain>>,
    server: tokio::task::JoinHandle<()>,
}

impl RpcFixture {
    async fn spawn(branch: u64, through: i64) -> Result<Self> {
        let state = Arc::new(RwLock::new(rpc_chain(branch, through)));
        let server_state = Arc::clone(&state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", post(chain_rpc))
                    .with_state(server_state),
            )
            .await
            .expect("live fixture RPC server");
        });
        Ok(Self {
            endpoint: format!("http://{address}/"),
            state,
            server,
        })
    }

    async fn reorg(&self, branch: u64, ancestor: i64, through: i64) {
        let mut state = self.state.write().await;
        apply_fixture_reorg(&mut state, branch, ancestor, through);
    }

    async fn reorg_after_next_number_batch(&self, branch: u64, ancestor: i64, through: i64) {
        self.state.write().await.reorg_after_number_batch = Some((branch, ancestor, through));
    }
}

fn rpc_chain(branch: u64, through: i64) -> RpcChain {
    let mut canonical = Vec::new();
    let mut blocks = BTreeMap::new();
    for number in 0..=through {
        let hash = block_hash(branch, number);
        let parent = if number == 0 {
            format!("0x{:064x}", 0)
        } else {
            block_hash(branch, number - 1)
        };
        blocks.insert(
            hash.clone(),
            json!({
                "hash": hash,
                "parentHash": parent,
                "number": format!("0x{number:x}"),
                "timestamp": format!("0x{:x}", number + 1),
                "logsBloom": "0x",
                "transactions": []
            }),
        );
        canonical.push(hash);
    }
    RpcChain {
        canonical,
        blocks,
        reorg_after_number_batch: None,
    }
}

async fn chain_rpc(
    State(state): State<Arc<RwLock<RpcChain>>>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let mut state = state.write().await;
    let response = if let Some(requests) = request.as_array() {
        Value::Array(
            requests
                .iter()
                .map(|request| chain_rpc_response(&state, request))
                .collect(),
        )
    } else {
        chain_rpc_response(&state, &request)
    };
    let is_number_batch = request.as_array().is_some_and(|requests| {
        requests.iter().all(|request| {
            request["method"] == "eth_getBlockByNumber"
                && request
                    .pointer("/params/0")
                    .and_then(Value::as_str)
                    .is_some_and(|selector| selector.starts_with("0x"))
        })
    });
    if is_number_batch
        && let Some((branch, ancestor, through)) = state.reorg_after_number_batch.take()
    {
        apply_fixture_reorg(&mut state, branch, ancestor, through);
    }
    Json(response)
}

fn apply_fixture_reorg(state: &mut RpcChain, branch: u64, ancestor: i64, through: i64) {
    state.canonical.truncate((ancestor + 1) as usize);
    for number in ancestor + 1..=through {
        let hash = block_hash(branch, number);
        let parent = state
            .canonical
            .last()
            .cloned()
            .expect("fixture reorg retains its ancestor");
        state.blocks.insert(
            hash.clone(),
            json!({
                "hash": hash,
                "parentHash": parent,
                "number": format!("0x{number:x}"),
                "timestamp": format!("0x{:x}", number + 1),
                "logsBloom": "0x",
                "transactions": []
            }),
        );
        state.canonical.push(hash);
    }
}

fn chain_rpc_response(state: &RpcChain, request: &Value) -> Value {
    let id = request.get("id").cloned().unwrap_or(json!(1));
    let params = request["params"].as_array().cloned().unwrap_or_default();
    let result = match request["method"].as_str().unwrap_or_default() {
        "eth_getBlockByNumber" => {
            let selector = params.first().and_then(Value::as_str).unwrap_or_default();
            let index = match selector {
                "latest" => state.canonical.len().checked_sub(1),
                "safe" | "finalized" => Some(0),
                quantity => usize::from_str_radix(quantity.trim_start_matches("0x"), 16).ok(),
            };
            index
                .and_then(|index| state.canonical.get(index))
                .and_then(|hash| state.blocks.get(hash))
                .cloned()
        }
        "eth_getBlockByHash" => params
            .first()
            .and_then(Value::as_str)
            .and_then(|hash| state.blocks.get(hash))
            .cloned(),
        "eth_getLogs" => Some(json!([])),
        _ => None,
    };
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

struct HydrationRpc {
    endpoint: String,
    server: tokio::task::JoinHandle<()>,
}

impl HydrationRpc {
    async fn spawn(values: BTreeMap<String, String>) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let values = Arc::new(values);
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", post(hydration_rpc))
                    .with_state(values),
            )
            .await
            .expect("hydration fixture RPC server");
        });
        Ok(Self {
            endpoint: format!("http://{address}/"),
            server,
        })
    }
}

async fn hydration_rpc(
    State(values): State<Arc<BTreeMap<String, String>>>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let id = request.get("id").cloned().unwrap_or(json!(1));
    let block_hash = request
        .pointer("/params/1/blockHash")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let value = values.get(block_hash).cloned().unwrap_or_default();
    if value == FAILED_MULTICALL_BATCH {
        return Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": -32000, "message": "fixture multicall failure"}
        }));
    }
    let result = if let Some(values) = value.strip_prefix(MULTICALL_RESULTS_PREFIX) {
        multicall_string_results(values.split('|'))
    } else if value == FAILED_MULTICALL {
        multicall_failed_result()
    } else {
        multicall_string_result(&value)
    };
    Json(json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result
    }))
}

fn multicall_string_result(value: &str) -> String {
    multicall_string_results([value])
}

fn multicall_string_results<'a>(values: impl IntoIterator<Item = &'a str>) -> String {
    let results = values
        .into_iter()
        .map(|value| {
            let inner = (value.to_owned(),).abi_encode_params();
            (true, Bytes::from(inner))
        })
        .collect::<Vec<_>>();
    let outer = (results,).abi_encode_params();
    format!("0x{}", alloy_primitives::hex::encode(outer))
}

fn multicall_failed_result() -> String {
    let outer = (vec![(false, Bytes::new())],).abi_encode_params();
    format!("0x{}", alloy_primitives::hex::encode(outer))
}
