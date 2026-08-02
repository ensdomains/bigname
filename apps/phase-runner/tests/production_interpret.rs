#[allow(dead_code)]
mod support;

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use anyhow::Result;
use bigname_ingest::load_watch_filter;
use bigname_interpret::{
    BatchRequest, Engine, ErrorKind as InterpretErrorKind, Marker, RunMode as InterpretRunMode,
};
use phase_runner::{
    INTERPRETER_CONTENT_HASH,
    capacity::CapacityGuard,
    config::{CapacityConfig, ChainConfig, SeedBasis, SourceConfig, TimingConfig},
    error::ErrorKind,
    interpret_phase::InterpretPhase,
    phase::{BlockRange, LoopbackPhase, PhaseName, PhaseSet},
    runner::{PhaseRunner, RedoPhase},
    state::{PhaseStore, StartDisposition},
};
use serde_json::json;
use sqlx::{
    PgPool,
    postgres::PgPoolOptions,
    types::{Uuid, time},
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use support::ScratchDatabase;

const CONTRACT: &str = "0x0000000000000000000000000000000000000042";
const DISCOVERED_RESOLVER: &str = "0x0000000000000000000000000000000000000044";
const ANNOUNCED_REGISTRY: &str = "0x0000000000000000000000000000000000000045";
const SENDER: &str = "0x0000000000000000000000000000000000000043";
const NORMALIZER: &str = "ensip15@ens-normalize-0.1.1";
type DiscoveryEpochRow = (i64, Option<i64>, bool, Option<String>, Option<String>);

sol! {
    event NameRegistered(
        string name,
        bytes32 indexed label,
        address indexed owner,
        uint256 expires
    );
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
    event RegistryCreated();
    event NameWrapped(
        bytes32 indexed node,
        bytes name,
        address owner,
        uint32 fuses,
        uint64 expiry
    );
}

mod v2_registry_events {
    use alloy_sol_types::sol;

    sol! {
        event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender);
        event LabelUnregistered(uint256 indexed tokenId, address indexed sender);
        event TokenResource(uint256 indexed tokenId, uint256 indexed resource);
        event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap);
        event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
    }
}

#[tokio::test]
async fn production_interpret_writes_plain_events_identity_preimages_and_flags() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret").await?;
    seed_fixture(scratch.pool(), "interpret-basic", &[(1, "alice")]).await?;

    run_engine(
        scratch.pool(),
        "interpret-basic",
        0,
        1,
        InterpretRunMode::Normal,
    )
    .await?;

    let event_kinds: Vec<String> =
        sqlx::query_scalar("SELECT event_kind FROM normalized_events ORDER BY normalized_event_id")
            .fetch_all(scratch.pool())
            .await?;
    assert_eq!(
        event_kinds,
        [
            "RegistrationGranted",
            "ExpiryChanged",
            "PermissionChanged",
            "SurfaceBound",
            "AuthorityEpochChanged",
            "PreimageObserved"
        ]
    );
    let surface: (String, String, Vec<String>, String) = sqlx::query_as(
        "
        SELECT raw_name, visibility_state, raw_labels, logical_name_id
        FROM name_surfaces
        ",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(surface.0, "alice.eth");
    assert_eq!(surface.1, "active");
    assert_eq!(surface.2, ["alice", "eth"]);
    assert!(surface.3.starts_with("ens:0x"));
    assert!(!surface.3.contains("alice"));
    let preimages: Vec<(String, bool, Option<String>)> = sqlx::query_as(
        "
        SELECT decoded_label, normalized_under_version, normalization_error
        FROM label_preimages
        ORDER BY decoded_label
        ",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        preimages,
        vec![("alice".into(), true, None), ("eth".into(), true, None)]
    );
    let binding_count: i64 = sqlx::query_scalar("SELECT count(*) FROM surface_bindings")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(binding_count, 1);
    scratch.cleanup().await
}

#[tokio::test]
async fn divergent_normalized_event_identity_errors_loudly() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_event_conflict").await?;
    let chain = "interpret-event-conflict";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    sqlx::query(
        "UPDATE normalized_events SET after_state = '{\"tampered\":true}'::jsonb WHERE chain_id = $1 AND event_kind = 'RegistrationGranted'",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;

    let error = Engine::new(scratch.pool().clone())
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 1,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await
        .expect_err("a stable event identity must not overwrite divergent data");
    assert_eq!(error.kind(), InterpretErrorKind::DataIntegrity);
    assert!(error.to_string().contains("different event data"));
    scratch.cleanup().await
}

#[tokio::test]
async fn normalized_event_replay_accepts_only_canonicality_lifecycle_changes() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_event_canonicality").await?;
    let chain = "interpret-event-canonicality";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'safe' WHERE chain_id = $1 AND block_number = 1",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    let advanced: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT canonicality_state::text FROM normalized_events WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(advanced, ["safe"]);

    let recanonicalized_chain = "interpret-event-recanonicalized";
    seed_fixture(scratch.pool(), recanonicalized_chain, &[(1, "bob")]).await?;
    run_engine(
        scratch.pool(),
        recanonicalized_chain,
        0,
        1,
        InterpretRunMode::Normal,
    )
    .await?;
    sqlx::query("UPDATE normalized_events SET canonicality_state = 'orphaned' WHERE chain_id = $1")
        .bind(recanonicalized_chain)
        .execute(scratch.pool())
        .await?;
    run_engine(
        scratch.pool(),
        recanonicalized_chain,
        0,
        1,
        InterpretRunMode::Normal,
    )
    .await?;
    let recanonicalized: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT canonicality_state::text FROM normalized_events WHERE chain_id = $1",
    )
    .bind(recanonicalized_chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(recanonicalized, ["canonical"]);
    scratch.cleanup().await
}

#[tokio::test]
async fn divergent_token_lineage_chain_errors_loudly() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_lineage_conflict").await?;
    let chain = "interpret-lineage-conflict";
    let other_chain = "interpret-lineage-conflict-other";
    seed_v2_lifecycle_fixture(scratch.pool(), chain).await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;
    sqlx::query("DELETE FROM normalized_events WHERE chain_id = $1")
        .bind(chain)
        .execute(scratch.pool())
        .await?;
    sqlx::query("DELETE FROM surface_bindings WHERE chain_id = $1")
        .bind(chain)
        .execute(scratch.pool())
        .await?;
    sqlx::query("DELETE FROM resources WHERE chain_id = $1")
        .bind(chain)
        .execute(scratch.pool())
        .await?;
    sqlx::query(
        "
        INSERT INTO chain_lineage (
            chain_id, block_hash, block_number, block_timestamp, canonicality_state
        )
        VALUES ($1, $2, 1, to_timestamp(1), 'canonical')
        ",
    )
    .bind(other_chain)
    .bind(block_hash(other_chain, 1))
    .execute(scratch.pool())
    .await?;
    sqlx::query("UPDATE token_lineages SET chain_id = $2, block_hash = $3 WHERE chain_id = $1")
        .bind(chain)
        .bind(other_chain)
        .bind(block_hash(other_chain, 1))
        .execute(scratch.pool())
        .await?;

    let error = Engine::new(scratch.pool().clone())
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 2,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await
        .expect_err("a token lineage ID must not move across chains");
    assert_eq!(error.kind(), InterpretErrorKind::DataIntegrity);
    assert!(error.to_string().contains("different chain"));
    scratch.cleanup().await
}

#[tokio::test]
async fn divergent_token_lineage_provenance_errors_loudly() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_lineage_data_conflict").await?;
    let chain = "interpret-lineage-data-conflict";
    seed_v2_lifecycle_fixture(scratch.pool(), chain).await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;
    sqlx::query(
        "
        UPDATE token_lineages
        SET provenance = jsonb_set(provenance, '{log_index}', '-1'::jsonb)
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;

    let error = Engine::new(scratch.pool().clone())
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 2,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await
        .expect_err("a token lineage ID must not overwrite divergent same-chain data");
    assert_eq!(error.kind(), InterpretErrorKind::DataIntegrity);
    assert!(error.to_string().contains("different lineage data"));
    scratch.cleanup().await
}

#[tokio::test]
async fn nonzero_full_replay_rejects_a_forged_prior_token_lineage_anchor() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_lineage_coverage_conflict").await?;
    let chain = "interpret-lineage-coverage-conflict";
    seed_fixture(scratch.pool(), chain, &[(100, "alice")]).await?;
    run_engine(scratch.pool(), chain, 100, 100, InterpretRunMode::Normal).await?;
    sqlx::query(
        "
        UPDATE token_lineages
        SET block_hash = $2,
            block_number = 99,
            provenance = $3
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .bind(block_hash(chain, 99))
    .bind(json!({
        "source": "raw_log",
        "chain_id": chain,
        "block_hash": block_hash(chain, 99),
        "block_number": 99,
        "transaction_index": 0,
        "log_index": 0,
    }))
    .execute(scratch.pool())
    .await?;

    let error = Engine::new(scratch.pool().clone())
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 100,
            to_block: 100,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await
        .expect_err("full replay must reject a forged pre-coverage lineage anchor");
    assert_eq!(error.kind(), InterpretErrorKind::DataIntegrity);
    assert!(error.to_string().contains("different lineage data"));
    scratch.cleanup().await
}

#[tokio::test]
async fn later_batch_token_lineage_observation_preserves_the_first_anchor() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_lineage_later_batch").await?;
    let chain = "interpret-lineage-later-batch";
    seed_v2_lifecycle_fixture(scratch.pool(), chain).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    let before: (i64, serde_json::Value) =
        sqlx::query_as("SELECT block_number, provenance FROM token_lineages WHERE chain_id = $1")
            .bind(chain)
            .fetch_one(scratch.pool())
            .await?;

    run_engine(scratch.pool(), chain, 2, 2, InterpretRunMode::Normal).await?;
    let after: (i64, serde_json::Value) =
        sqlx::query_as("SELECT block_number, provenance FROM token_lineages WHERE chain_id = $1")
            .bind(chain)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(after, before);
    scratch.cleanup().await
}

#[tokio::test]
async fn cross_batch_prior_state_uses_hash_covered_compaction_keys() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_prior_state_keys").await?;
    let chain = "interpret-prior-state-keys";
    seed_fixture(scratch.pool(), chain, &[(1, "alice"), (2, "alice")]).await?;

    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    run_engine(scratch.pool(), chain, 2, 2, InterpretRunMode::Normal).await?;

    let counts: (i64, i64) = sqlx::query_as(
        "
        SELECT count(*),
               count(*) FILTER (
                   WHERE raw_fact_ref ? 'interpreter_state_key'
               )
        FROM normalized_events
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert!(counts.0 > 0);
    assert_eq!(
        counts.1, counts.0,
        "every adapter event must carry its hash-covered prior-state compaction key"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn prior_state_is_loaded_once_and_folded_forward_across_500_block_batches() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_prior_state_session").await?;
    let chain = "interpret-prior-state-session";
    seed_fixture(scratch.pool(), chain, &[(1, "alice"), (501, "alice")]).await?;
    let engine = Engine::new(scratch.pool().clone());
    let first = engine
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 501,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    assert!(!first.complete);
    assert_eq!(first.current.number, 499);
    sqlx::query("DELETE FROM normalized_events WHERE chain_id = $1")
        .bind(chain)
        .execute(scratch.pool())
        .await?;

    let second = engine
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 501,
            resume_current: Some(first.current),
            mode: InterpretRunMode::Normal,
        })
        .await?;
    assert!(second.complete);
    let prior_registrant: Option<String> = sqlx::query_scalar(
        "
        SELECT before_state ->> 'registrant'
        FROM normalized_events
        WHERE chain_id = $1
          AND block_number = 501
          AND event_kind = 'RegistrationGranted'
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(prior_registrant.as_deref(), Some(CONTRACT));
    scratch.cleanup().await
}

#[tokio::test]
async fn cached_prior_state_reloads_when_an_earlier_dependency_is_repaired() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_prior_dependency_repair").await?;
    let chain = "interpret-prior-dependency-repair";
    seed_fixture(scratch.pool(), chain, &[(1, "alice"), (501, "alice")]).await?;
    insert_orphaned_registration(scratch.pool(), chain, 1, "alice").await?;
    let engine = Engine::new(scratch.pool().clone());
    let first = engine
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 501,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    assert_eq!(first.current.number, 499);

    sqlx::query(
        "
        UPDATE chain_lineage
        SET canonicality_state = CASE
            WHEN block_hash = $2 THEN 'canonical'::canonicality_state
            ELSE 'orphaned'::canonicality_state
        END
        WHERE chain_id = $1 AND block_number = 1
        ",
    )
    .bind(chain)
    .bind(format!("{chain}-orphan-1"))
    .execute(scratch.pool())
    .await?;

    let second = engine
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 501,
            resume_current: Some(first.current),
            mode: InterpretRunMode::Normal,
        })
        .await?;
    assert!(second.complete);
    let prior_registrant: Option<String> = sqlx::query_scalar(
        "
        SELECT before_state ->> 'registrant'
        FROM normalized_events
        WHERE chain_id = $1
          AND block_number = 501
          AND event_kind = 'RegistrationGranted'
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(prior_registrant, None);
    scratch.cleanup().await
}

#[tokio::test]
async fn lapsed_reregistration_rotates_the_resource_lineage_and_binding() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_reregistration").await?;
    let chain = "interpret-reregistration";
    seed_fixture_with_timestamps(
        scratch.pool(),
        chain,
        &[(1, "alice"), (2, "alice")],
        &[(2, 10_000_000)],
    )
    .await?;

    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;

    let registration_resources: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT resource_id) FROM normalized_events \
         WHERE chain_id = $1 AND event_kind = 'RegistrationGranted'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    let registration_lineages: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT token_lineage_id) FROM resources WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    let bindings: Vec<(bool,)> = sqlx::query_as(
        "SELECT active_to IS NULL FROM surface_bindings \
         WHERE chain_id = $1 ORDER BY active_from",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(registration_resources, 2);
    assert_eq!(registration_lineages, 2);
    assert_eq!(bindings, vec![(false,), (true,)]);
    scratch.cleanup().await
}

#[tokio::test]
async fn interpret_redo_replays_the_dependent_suffix_through_the_recorded_head() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_redo").await?;
    let chain = "interpret-redo";
    seed_fixture(scratch.pool(), chain, &[(1, "alice"), (2, "bob")]).await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;
    sqlx::query(
        "
        UPDATE normalized_events
        SET before_state = '{\"tampered\":true}'::jsonb
        WHERE block_number IN (1, 2)
          AND event_kind = 'RegistrationGranted'
        ",
    )
    .execute(scratch.pool())
    .await?;

    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain).await?;
    seed_completed_phase_extent(scratch.pool(), chain, INTERPRETER_CONTENT_HASH).await?;
    let phases = PhaseSet::with_ingest_and_interpret(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(InterpretPhase::new(scratch.runner().pool().clone())),
    )?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "interpret-redo-runner",
        test_timing(),
    )?;
    runner
        .redo(
            &chain_config(chain)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await?;

    let states: Vec<(i64, bool)> = sqlx::query_as(
        "
        SELECT block_number, before_state ? 'tampered'
        FROM normalized_events
        WHERE event_kind = 'RegistrationGranted'
        ORDER BY block_number
        ",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(states, [(1, false), (2, false)]);
    let identity_states: Vec<String> =
        sqlx::query_scalar("SELECT canonicality_state::text FROM name_surfaces ORDER BY raw_name")
            .fetch_all(scratch.pool())
            .await?;
    assert_eq!(identity_states, ["canonical", "canonical"]);
    let state: (String, bool, Option<String>) = sqlx::query_as(
        "
        SELECT phase_status, redo_in_progress, input_content_hash
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_name = 'interpret'
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        state,
        (
            "completed".into(),
            false,
            Some(INTERPRETER_CONTENT_HASH.into())
        )
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn interpret_redo_rejects_a_range_without_ingest_cursor_coverage() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_redo_raw_presence").await?;
    let chain = "interpret-redo-raw-presence";
    seed_fixture(scratch.pool(), chain, &[(1, "alice"), (2, "bob")]).await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;

    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain).await?;
    seed_completed_phase_extent(scratch.pool(), chain, INTERPRETER_CONTENT_HASH).await?;
    sqlx::query(
        "
        UPDATE ingest_cursors
        SET next_block_number = 1,
            last_processed_block_number = NULL,
            last_processed_block_hash = NULL
        WHERE chain_id = $1
          AND source_key = 'source'
        ",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;

    let phases = PhaseSet::with_ingest_and_interpret(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(InterpretPhase::new(scratch.runner().pool().clone())),
    )?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "interpret-redo-raw-presence-runner",
        test_timing(),
    )?;

    let error = runner
        .redo(
            &chain_config(chain)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("interpret redo must prove the selected raw range was ingested");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("raw-data presence"));

    let registrations: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events WHERE event_kind = 'RegistrationGranted'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(registrations, 2);
    scratch.cleanup().await
}

#[tokio::test]
async fn interpret_loader_rejects_two_live_hashes_at_one_height() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_duplicate_live_loader").await?;
    let chain = "interpret-duplicate-live-loader";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    permit_duplicate_live_heights_for_corruption_test(scratch.pool()).await?;
    insert_competing_live_block(scratch.pool(), chain, 1).await?;

    let error = Engine::new(scratch.pool().clone())
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 1,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await
        .expect_err("the loader must reject ambiguous live lineage");
    assert_eq!(error.kind(), InterpretErrorKind::DataIntegrity);
    assert!(error.to_string().contains("multiple live-lineage hashes"));
    scratch.cleanup().await
}

#[tokio::test]
async fn interpret_redo_presence_rejects_two_live_hashes_at_one_height() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_duplicate_live_redo").await?;
    let chain = "interpret-duplicate-live-redo";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain).await?;
    seed_completed_phase_extent_at(scratch.pool(), chain, 1, INTERPRETER_CONTENT_HASH).await?;
    permit_duplicate_live_heights_for_corruption_test(scratch.pool()).await?;
    insert_competing_live_block(scratch.pool(), chain, 1).await?;
    let phases = PhaseSet::with_ingest_and_interpret(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(InterpretPhase::new(scratch.runner().pool().clone())),
    )?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "interpret-duplicate-live-redo-runner",
        test_timing(),
    )?;

    let error = runner
        .redo(
            &chain_config(chain)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("redo presence must reject ambiguous live lineage");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("multiple hashes at one height"));
    scratch.cleanup().await
}

#[tokio::test]
async fn interpret_redo_requires_every_applicable_source_cursor_to_cover_the_suffix() -> Result<()>
{
    let scratch = ScratchDatabase::create("production_interpret_redo_source_presence").await?;
    let chain = "interpret-redo-source-presence";
    seed_fixture(scratch.pool(), chain, &[(1, "alice"), (2, "bob")]).await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain).await?;
    seed_completed_phase_extent(scratch.pool(), chain, INTERPRETER_CONTENT_HASH).await?;
    let configured = ChainConfig::new(
        chain,
        vec![
            SourceConfig::new(
                chain,
                "source",
                "test",
                SeedBasis::EthereumHead,
                0,
                "http://source.invalid",
            )?,
            SourceConfig::new(
                chain,
                "source-b",
                "test",
                SeedBasis::EthereumHead,
                0,
                "http://source-b.invalid",
            )?,
        ],
        false,
    )?;
    let phases = PhaseSet::with_ingest_and_interpret(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(InterpretPhase::new(scratch.runner().pool().clone())),
    )?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "interpret-redo-source-presence-runner",
        test_timing(),
    )?;

    let missing = runner
        .redo(
            &configured,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("each applicable configured source needs its own cursor");
    assert!(missing.to_string().contains("source-b has no cursor"));

    sqlx::query(
        "INSERT INTO ingest_cursors (
             chain_id, source_key, source_kind, seed_basis, start_block_number,
             next_block_number, target_block_number
         ) VALUES ($1, 'source-b', 'test', 'ethereum_head', 0, 1, 2)",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    let behind = runner
        .redo(
            &configured,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("one advanced source must not mask a behind source");
    assert!(behind.to_string().contains("source-b covers through 0"));
    scratch.cleanup().await
}

#[tokio::test]
async fn interpret_redo_is_blocked_by_a_partial_old_content_hash_epoch() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_hash_gate").await?;
    let chain_id = "interpret-hash-gate";
    seed_fixture(scratch.pool(), chain_id, &[(1, "alice"), (2, "bob")]).await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    seed_completed_phase_extent(scratch.pool(), chain_id, "keccak256:older-binary").await?;
    let phases = PhaseSet::with_ingest_and_interpret(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(InterpretPhase::new(scratch.runner().pool().clone())),
    )?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "interpret-hash-gate-runner",
        test_timing(),
    )?;

    let error = runner
        .redo(
            &chain_config(chain_id)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("a partial redo must not adopt a changed interpreter hash");
    assert_eq!(error.kind(), ErrorKind::ContentHashMismatch);
    assert!(error.to_string().contains("full range 0..=2"));
    scratch.cleanup().await
}

#[tokio::test]
async fn unnormalizable_raw_label_creates_a_deactivated_shadow_identity() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_shadow").await?;
    let chain = "interpret-shadow";
    seed_fixture(scratch.pool(), chain, &[(1, "bad\u{1}")]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    let surface: (String, String, Option<String>, bool) = sqlx::query_as(
        "
        SELECT raw_name,
               visibility_state,
               deactivation_reason,
               deactivated_at IS NOT NULL
        FROM name_surfaces
        ",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        surface,
        (
            "bad\u{1}.eth".into(),
            "shadow".into(),
            Some("normalization_gate".into()),
            true
        )
    );
    let flag: (bool, bool) = sqlx::query_as(
        "
        SELECT normalized_under_version, normalization_error IS NOT NULL
        FROM label_preimages
        WHERE decoded_label = $1
        ",
    )
    .bind("bad\u{1}")
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(flag, (false, true));
    let binding_count: i64 = sqlx::query_scalar("SELECT count(*) FROM surface_bindings")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(binding_count, 0);
    scratch.cleanup().await
}

#[tokio::test]
async fn non_utf8_label_persists_raw_bytes_and_completes_as_shadow() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_non_utf8_shadow").await?;
    let chain = "interpret-non-utf8-shadow";
    let raw_label = vec![0xff];
    seed_hostile_wrapper_fixture(scratch.pool(), chain, &raw_label).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    assert_hostile_label(scratch.pool(), &raw_label, None).await?;
    scratch.cleanup().await
}

#[tokio::test]
async fn embedded_dot_label_persists_bytes_and_completes_as_shadow() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_dot_shadow").await?;
    let chain = "interpret-dot-shadow";
    seed_fixture(scratch.pool(), chain, &[(1, "a.b")]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    assert_hostile_label(scratch.pool(), b"a.b", Some("a.b")).await?;
    scratch.cleanup().await
}

#[tokio::test]
async fn embedded_nul_label_persists_bytes_and_completes_as_shadow() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_nul_shadow").await?;
    let chain = "interpret-nul-shadow";
    let label = "a\0b";
    seed_fixture(scratch.pool(), chain, &[(1, label), (2, "alice")]).await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;

    let stored: (Vec<u8>, Option<String>, bool) = sqlx::query_as(
        "
        SELECT raw_label, decoded_label, normalized_under_version
        FROM label_preimages
        WHERE raw_label = $1
        ",
    )
    .bind(label.as_bytes())
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(stored, (label.as_bytes().to_vec(), None, false));

    let identities: Vec<(String, String)> = sqlx::query_as(
        "
        SELECT raw_name, visibility_state
        FROM name_surfaces
        ORDER BY raw_name
        ",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        identities,
        [
            (String::new(), "shadow".into()),
            ("alice.eth".into(), "active".into())
        ]
    );
    let binding_count: i64 = sqlx::query_scalar("SELECT count(*) FROM surface_bindings")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(binding_count, 1);
    scratch.cleanup().await
}

#[tokio::test]
async fn two_hundred_fifty_six_byte_label_completes_as_shadow() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_256_byte_shadow").await?;
    let chain = "interpret-256-byte-shadow";
    let label = "a".repeat(256);
    seed_fixture(scratch.pool(), chain, &[(1, &label)]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    assert_hostile_label(scratch.pool(), label.as_bytes(), Some(&label)).await?;
    scratch.cleanup().await
}

async fn assert_hostile_label(
    pool: &PgPool,
    raw_label: &[u8],
    decoded_label: Option<&str>,
) -> Result<()> {
    let stored: (Vec<u8>, Option<String>, bool, bool) = sqlx::query_as(
        "
        SELECT raw_label, decoded_label, normalized_under_version,
               normalization_error IS NOT NULL
        FROM label_preimages
        WHERE raw_label = $1
        ",
    )
    .bind(raw_label)
    .fetch_one(pool)
    .await?;
    assert_eq!(stored.0, raw_label);
    assert_eq!(stored.1.as_deref(), decoded_label);
    assert_eq!((stored.2, stored.3), (false, true));
    let (surfaces, shadow_surfaces, bindings): (i64, i64, i64) = sqlx::query_as(
        "
        SELECT (SELECT count(*) FROM name_surfaces),
               (SELECT count(*) FROM name_surfaces
                WHERE visibility_state = 'shadow'
                  AND deactivation_reason = 'normalization_gate'
                  AND deactivated_at IS NOT NULL),
               (SELECT count(*) FROM surface_bindings)
        ",
    )
    .fetch_one(pool)
    .await?;
    assert_eq!((surfaces, shadow_surfaces, bindings), (1, 1, 0));
    let shadow_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events WHERE after_state ->> 'visibility_state' = 'shadow'",
    )
    .fetch_one(pool)
    .await?;
    assert_eq!(shadow_events, 1);
    Ok(())
}

#[tokio::test]
async fn normalization_collision_keeps_raw_preimages_distinct_and_binds_only_visible_identity()
-> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_collision").await?;
    let chain = "interpret-collision";
    seed_fixture(scratch.pool(), chain, &[(1, "Alice"), (2, "alice")]).await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;

    let raw_labels: Vec<(String, String, bool)> = sqlx::query_as(
        "
        SELECT decoded_label, labelhash, normalized_under_version
        FROM label_preimages
        WHERE decoded_label IN ('Alice', 'alice')
        ORDER BY decoded_label
        ",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(raw_labels.len(), 2);
    assert_ne!(raw_labels[0].1, raw_labels[1].1);
    assert_eq!(
        raw_labels.iter().map(|row| row.2).collect::<Vec<_>>(),
        [false, true]
    );
    let visibility: Vec<(String, String)> =
        sqlx::query_as("SELECT raw_name, visibility_state FROM name_surfaces ORDER BY raw_name")
            .fetch_all(scratch.pool())
            .await?;
    assert_eq!(
        visibility,
        [
            ("Alice.eth".into(), "shadow".into()),
            ("alice.eth".into(), "active".into())
        ]
    );
    let binding_count: i64 = sqlx::query_scalar("SELECT count(*) FROM surface_bindings")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(binding_count, 1);
    scratch.cleanup().await
}

#[tokio::test]
async fn recompute_flags_is_unavailable_until_binding_reconciliation_exists() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_flags").await?;
    let chain = "interpret-flags";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    sqlx::query(
        "
        UPDATE label_preimages
        SET normalized_under_version = false,
            normalization_error = 'stale flag',
            normalizer_version = 'stale-version'
        WHERE decoded_label = 'alice'
        ",
    )
    .execute(scratch.pool())
    .await?;
    let event_count: i64 = sqlx::query_scalar("SELECT count(*) FROM normalized_events")
        .fetch_one(scratch.pool())
        .await?;

    let error = Engine::new(scratch.pool().clone())
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 1,
            resume_current: None,
            mode: InterpretRunMode::RecomputeFlags,
        })
        .await
        .expect_err("flag recompute must remain unavailable without binding reconciliation");
    assert_eq!(error.kind(), bigname_interpret::ErrorKind::Configuration);
    assert!(error.to_string().contains("binding reconciliation"));

    let flag: (String, bool, Option<String>) = sqlx::query_as(
        "
        SELECT normalizer_version, normalized_under_version, normalization_error
        FROM label_preimages
        WHERE decoded_label = 'alice'
        ",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(flag.0, "stale-version");
    let after_count: i64 = sqlx::query_scalar("SELECT count(*) FROM normalized_events")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(event_count, after_count);
    scratch.cleanup().await
}

#[tokio::test]
async fn normal_mode_rejects_stored_normalizer_state_drift() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_normalizer_drift").await?;
    let chain = "interpret-normalizer-drift";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    sqlx::query(
        "
        UPDATE label_preimages
        SET normalizer_version = 'stale-version',
            normalized_under_version = false,
            normalization_error = 'stale flag'
        WHERE decoded_label = 'alice'
        ",
    )
    .execute(scratch.pool())
    .await?;
    let preimage_error = Engine::new(scratch.pool().clone())
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 1,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await
        .expect_err("normal mode must not publish over stale preimage normalization state");
    assert_eq!(preimage_error.kind(), InterpretErrorKind::DataIntegrity);
    assert!(preimage_error.to_string().contains("recompute-flags"));
    let preimage: (String, bool, Option<String>) = sqlx::query_as(
        "
        SELECT normalizer_version, normalized_under_version, normalization_error
        FROM label_preimages
        WHERE decoded_label = 'alice'
        ",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        preimage,
        ("stale-version".into(), false, Some("stale flag".into()))
    );

    sqlx::query(
        "
        UPDATE label_preimages
        SET normalizer_version = $1,
            normalized_under_version = true,
            normalization_error = NULL
        WHERE decoded_label = 'alice'
        ",
    )
    .bind(NORMALIZER)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        r#"
        UPDATE name_surfaces
        SET normalizer_version = 'stale-version',
            visibility_state = 'shadow',
            normalization_errors = '[{"error":"stale flag"}]'::jsonb,
            deactivation_reason = 'normalization_gate',
            deactivated_at = now()
        WHERE chain_id = $1
        "#,
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    let surface_error = Engine::new(scratch.pool().clone())
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 1,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await
        .expect_err("normal mode must not publish over stale surface normalization state");
    assert_eq!(surface_error.kind(), InterpretErrorKind::DataIntegrity);
    assert!(surface_error.to_string().contains("recompute-flags"));
    let surface: (String, String, serde_json::Value, Option<String>, bool) = sqlx::query_as(
        "
        SELECT normalizer_version, visibility_state, normalization_errors,
               deactivation_reason, deactivated_at IS NOT NULL
        FROM name_surfaces
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(surface.0, "stale-version");
    assert_eq!(surface.1, "shadow");
    assert_eq!(surface.2, json!([{"error":"stale flag"}]));
    assert_eq!(surface.3.as_deref(), Some("normalization_gate"));
    assert!(surface.4);
    let active_bindings: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM surface_bindings WHERE chain_id = $1 AND active_to IS NULL",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(active_bindings, 1);
    scratch.cleanup().await
}

#[tokio::test]
async fn unavailable_recompute_flags_does_not_claim_a_redo_session() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_flags_redo_state").await?;
    let chain = "interpret-flags-redo-state";
    seed_fixture(scratch.pool(), chain, &[(1, "alice"), (2, "bob")]).await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain).await?;
    seed_completed_phase_extent(scratch.pool(), chain, INTERPRETER_CONTENT_HASH).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed',
             current_block_number = 2,
             current_block_hash = $2,
             target_block_number = 2,
             target_block_hash = $2,
             input_content_hash = $3,
             started_at = now(),
             finished_at = now()
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(chain)
    .bind(block_hash(chain, 2))
    .bind(INTERPRETER_CONTENT_HASH)
    .execute(scratch.pool())
    .await?;
    let phases = PhaseSet::with_ingest_and_interpret(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(InterpretPhase::new(scratch.runner().pool().clone())),
    )?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "interpret-flags-redo-state-runner",
        test_timing(),
    )?;

    let error = runner
        .redo(
            &chain_config(chain)?,
            RedoPhase::RecomputeFlags,
            BlockRange::new(0, 2)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("the unavailable mode must stop before claiming redo state");
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert!(error.to_string().contains("binding reconciliation"));

    let state: (String, bool, Option<String>) = sqlx::query_as(
        "SELECT phase_status, redo_in_progress, last_error
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(state, ("completed".to_owned(), false, None));
    assert_eq!(
        store
            .start_phase(
                chain,
                PhaseName::Interpret,
                &phase_runner::phase::RunMode::Normal
            )
            .await?,
        StartDisposition::AlreadyCompleted
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn registry_announcement_round_trips_to_a_forward_only_watch_range() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_announcement").await?;
    let chain = "interpret-announcement";
    seed_announcement_fixture(scratch.pool(), chain).await?;

    let topic0 = format!("{:#x}", RegistryCreated::SIGNATURE_HASH);
    let intake_watch = load_watch_filter(scratch.pool(), chain, 0, 1).await?;
    assert!(
        intake_watch.includes(ANNOUNCED_REGISTRY, &topic0, 1),
        "RegistryCreated must be collected from an address that is not admitted yet"
    );
    assert!(intake_watch.queries().iter().any(|query| {
        query.addresses.is_empty()
            && query.from_block == 0
            && query.to_block == 1
            && query.topic0s.iter().any(|topic| topic == &topic0)
    }));
    let watched_topic = format!("{:#x}", v2_registry_events::LabelRegistered::SIGNATURE_HASH);
    let provisional_watch = load_watch_filter(scratch.pool(), chain, 0, 5).await?;
    assert!(!provisional_watch.includes(ANNOUNCED_REGISTRY, &watched_topic, 0));
    assert!(provisional_watch.includes(ANNOUNCED_REGISTRY, &watched_topic, 1));
    assert!(provisional_watch.includes(ANNOUNCED_REGISTRY, &watched_topic, 5));

    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    let edge: (String, Uuid, Uuid, i64, String) = sqlx::query_as(
        "
        SELECT edge.edge_kind,
               edge.from_contract_instance_id,
               edge.to_contract_instance_id,
               edge.active_from_block_number,
               address.address
        FROM discovery_edges edge
        JOIN contract_instance_addresses address
          ON address.contract_instance_id = edge.to_contract_instance_id
         AND address.chain_id = edge.chain_id
        WHERE edge.chain_id = $1
          AND edge.edge_kind = 'registry_announcement'
          AND edge.deactivated_at IS NULL
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(edge.0, "registry_announcement");
    assert_eq!(edge.1, edge.2);
    assert_eq!(edge.3, 1);
    assert_eq!(edge.4, ANNOUNCED_REGISTRY);

    let watch = load_watch_filter(scratch.pool(), chain, 0, 5).await?;
    assert!(!watch.includes(ANNOUNCED_REGISTRY, &watched_topic, 0));
    assert!(watch.includes(ANNOUNCED_REGISTRY, &watched_topic, 1));
    assert!(watch.includes(ANNOUNCED_REGISTRY, &watched_topic, 5));
    assert!(watch.queries().iter().any(|query| {
        query.from_block == 1
            && query.to_block == 5
            && query
                .addresses
                .iter()
                .any(|address| address == ANNOUNCED_REGISTRY)
    }));
    scratch.cleanup().await
}

#[tokio::test]
async fn subregistry_topology_edge_does_not_expand_the_watch_plan() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_subregistry_topology").await?;
    let chain = "interpret-subregistry-topology";
    seed_v2_lifecycle_fixture(scratch.pool(), chain).await?;
    let (manifest_id, registry_id): (i64, Uuid) = sqlx::query_as(
        "
        SELECT manifest.manifest_id, declaration.contract_instance_id
        FROM manifest_versions manifest
        JOIN manifest_contract_instances declaration
          ON declaration.manifest_id = manifest.manifest_id
        WHERE manifest.chain_id = $1
          AND declaration.role = 'registry'
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    let child_id = Uuid::new_v4();
    let child_address = "0x0000000000000000000000000000000000000077";
    sqlx::query(
        "
        INSERT INTO contract_instances (
            contract_instance_id, chain_id, contract_kind, provenance
        )
        VALUES ($1, $2, 'contract', '{}'::jsonb)
        ",
    )
    .bind(child_id)
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "
        INSERT INTO contract_instance_addresses (
            contract_instance_id, chain_id, address,
            active_from_block_number, source_manifest_id, provenance
        )
        VALUES ($1, $2, $3, 1, $4, '{}'::jsonb)
        ",
    )
    .bind(child_id)
    .bind(chain)
    .bind(child_address)
    .bind(manifest_id)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "
        INSERT INTO discovery_edges (
            chain_id, edge_kind, from_contract_instance_id,
            to_contract_instance_id, discovery_source, admission_basis,
            source_manifest_id, active_from_block_number,
            active_from_block_hash, canonicality_state, provenance
        )
        VALUES (
            $1, 'subregistry', $2, $3, 'SubregistryUpdated',
            'linked_subregistry_event', $4, 1, $5, 'canonical', '{}'::jsonb
        )
        ",
    )
    .bind(chain)
    .bind(registry_id)
    .bind(child_id)
    .bind(manifest_id)
    .bind(block_hash(chain, 1))
    .execute(scratch.pool())
    .await?;

    let topic0 = format!("{:#x}", v2_registry_events::LabelRegistered::SIGNATURE_HASH);
    let watch = load_watch_filter(scratch.pool(), chain, 0, 3).await?;
    assert!(!watch.includes(child_address, &topic0, 1));
    scratch.cleanup().await
}

#[tokio::test]
async fn ens_v2_root_resolver_discovery_uses_resolver_manifest_watch_topics() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_root_resolver_watch").await?;
    let chain = "interpret-root-resolver-watch";
    seed_root_resolver_watch_fixture(scratch.pool(), chain).await?;

    let record_topic = format!("{:#x}", TextChanged::SIGNATURE_HASH);
    let watch = load_watch_filter(scratch.pool(), chain, 0, 5).await?;

    assert!(!watch.includes(DISCOVERED_RESOLVER, &record_topic, 0));
    assert!(
        watch.includes(DISCOVERED_RESOLVER, &record_topic, 1),
        "a root-discovered resolver must use ens_v2_resolver_l1 event topics"
    );
    assert!(watch.includes(DISCOVERED_RESOLVER, &record_topic, 5));
    scratch.cleanup().await
}

#[tokio::test]
async fn discovery_admission_applies_to_later_logs_in_the_same_batch() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_discovery").await?;
    let chain = "interpret-discovery";
    seed_discovery_fixture(scratch.pool(), chain).await?;

    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    let edge: (String, String) = sqlx::query_as(
        "
        SELECT edge.edge_kind, address.address
        FROM discovery_edges edge
        JOIN contract_instance_addresses address
          ON address.contract_instance_id = edge.to_contract_instance_id
        WHERE edge.chain_id = $1
          AND edge.deactivated_at IS NULL
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(edge, ("resolver".into(), DISCOVERED_RESOLVER.into()));
    let resolver_event: (String, i64) = sqlx::query_as(
        "
        SELECT source_family, count(*)
        FROM normalized_events
        WHERE chain_id = $1
          AND event_kind = 'RecordChanged'
        GROUP BY source_family
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(resolver_event, ("ens_v2_resolver_l1".into(), 1));
    scratch.cleanup().await
}

#[tokio::test]
async fn discovery_update_preserves_sibling_observation_key() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_discovery_siblings").await?;
    let chain = "interpret-discovery-siblings";
    seed_sibling_discovery_fixture(scratch.pool(), chain).await?;

    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;

    let active_targets: Vec<String> = sqlx::query_scalar(
        "
        SELECT lower(address.address)
        FROM discovery_edges edge
        JOIN contract_instance_addresses address
          ON address.contract_instance_id = edge.to_contract_instance_id
         AND address.chain_id = edge.chain_id
        WHERE edge.chain_id = $1
          AND edge.edge_kind = 'resolver'
          AND edge.deactivated_at IS NULL
        ORDER BY lower(address.address)
        ",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        active_targets,
        [
            "0x0000000000000000000000000000000000000052",
            "0x0000000000000000000000000000000000000053",
        ]
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn exact_discovery_redo_reuses_the_existing_edge_epoch() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_discovery_redo").await?;
    let chain = "interpret-discovery-redo";
    seed_discovery_fixture(scratch.pool(), chain).await?;

    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    let original_edge_id: i64 =
        sqlx::query_scalar("SELECT discovery_edge_id FROM discovery_edges WHERE chain_id = $1")
            .bind(chain)
            .fetch_one(scratch.pool())
            .await?;

    run_engine(scratch.pool(), chain, 1, 1, InterpretRunMode::Redo).await?;

    let edges: Vec<(i64, String, bool)> = sqlx::query_as(
        "
        SELECT discovery_edge_id, canonicality_state::text, deactivated_at IS NULL
        FROM discovery_edges
        WHERE chain_id = $1
        ORDER BY discovery_edge_id
        ",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(edges, vec![(original_edge_id, "canonical".into(), true)]);
    scratch.cleanup().await
}

#[tokio::test]
async fn partial_discovery_redo_caps_the_replayed_edge_at_its_surviving_successor() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_discovery_successor").await?;
    let chain = "interpret-discovery-successor";
    seed_sibling_discovery_fixture(scratch.pool(), chain).await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;

    run_engine(scratch.pool(), chain, 1, 1, InterpretRunMode::Redo).await?;

    let epochs: Vec<(String, i64, Option<i64>, bool)> = sqlx::query_as(
        "
        SELECT lower(address.address),
               edge.active_from_block_number,
               edge.active_to_block_number,
               edge.deactivated_at IS NULL
        FROM discovery_edges edge
        JOIN contract_instance_addresses address
          ON address.contract_instance_id = edge.to_contract_instance_id
         AND address.chain_id = edge.chain_id
        WHERE edge.chain_id = $1
          AND edge.edge_kind = 'resolver'
          AND lower(address.address) IN (
              '0x0000000000000000000000000000000000000051',
              '0x0000000000000000000000000000000000000053'
          )
          AND edge.canonicality_state IN ('canonical', 'safe', 'finalized')
        ORDER BY edge.active_from_block_number
        ",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        epochs,
        [
            (
                "0x0000000000000000000000000000000000000051".into(),
                1,
                Some(2),
                false,
            ),
            (
                "0x0000000000000000000000000000000000000053".into(),
                2,
                None,
                true,
            ),
        ]
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn discovery_open_redo_preserves_a_terminal_close_after_the_range() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_discovery_terminal").await?;
    let chain = "interpret-discovery-terminal";
    seed_discovery_fixture(scratch.pool(), chain).await?;
    sqlx::query(
        "
        INSERT INTO chain_lineage (
            chain_id, block_hash, parent_hash, block_number,
            block_timestamp, canonicality_state
        )
        VALUES ($1, $2, $3, 2, to_timestamp(2), 'canonical')
        ",
    )
    .bind(chain)
    .bind(block_hash(chain, 2))
    .bind(block_hash(chain, 1))
    .execute(scratch.pool())
    .await?;
    let transaction_hash = format!("{chain}-transaction-2");
    sqlx::query(
        "
        INSERT INTO raw_transactions (
            chain_id, block_hash, block_number, transaction_hash,
            transaction_index, from_address, to_address
        )
        VALUES ($1, $2, 2, $3, 0, $4, $5)
        ",
    )
    .bind(chain)
    .bind(block_hash(chain, 2))
    .bind(&transaction_hash)
    .bind(SENDER)
    .bind(CONTRACT)
    .execute(scratch.pool())
    .await?;
    let close = ResolverUpdated {
        tokenId: U256::from(7),
        resolver: Address::ZERO,
        sender: SENDER.parse()?,
    }
    .encode_log_data();
    insert_log_at(
        scratch.pool(),
        chain,
        2,
        &transaction_hash,
        0,
        CONTRACT,
        close.topics(),
        close.data.as_ref(),
    )
    .await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;

    run_engine(scratch.pool(), chain, 1, 1, InterpretRunMode::Redo).await?;

    let close: (Option<i64>, bool) = sqlx::query_as(
        "
        SELECT active_to_block_number, deactivated_at IS NOT NULL
        FROM discovery_edges
        WHERE chain_id = $1
          AND edge_kind = 'resolver'
          AND canonicality_state IN ('canonical', 'safe', 'finalized')
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(close, (Some(2), true));
    scratch.cleanup().await
}

#[tokio::test]
async fn discovery_backfill_does_not_cross_a_retained_address_epoch() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_address_epoch").await?;
    let chain = "interpret-address-epoch";
    seed_discovery_fixture(scratch.pool(), chain).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    sqlx::query(
        "
        UPDATE contract_instance_addresses
        SET active_from_block_number = 0,
            active_from_block_hash = NULL,
            active_to_block_number = 100,
            active_to_block_hash = NULL,
            deactivated_at = now()
        WHERE chain_id = $1
          AND lower(address) = lower($2)
        ",
    )
    .bind(chain)
    .bind(DISCOVERED_RESOLVER)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "
        INSERT INTO contract_instance_addresses (
            contract_instance_id, chain_id, address,
            active_from_block_number, source_manifest_id, provenance
        )
        SELECT contract_instance_id, chain_id, address, 101,
               source_manifest_id, jsonb_build_object('kind', 'manifest')
        FROM contract_instance_addresses
        WHERE chain_id = $1
          AND lower(address) = lower($2)
        ",
    )
    .bind(chain)
    .bind(DISCOVERED_RESOLVER)
    .execute(scratch.pool())
    .await?;

    run_engine(scratch.pool(), chain, 1, 1, InterpretRunMode::Redo).await?;

    let epochs: Vec<DiscoveryEpochRow> = sqlx::query_as(
        "
        SELECT active_from_block_number, active_to_block_number,
               deactivated_at IS NULL, active_from_block_hash,
               provenance ->> 'source'
        FROM contract_instance_addresses
        WHERE chain_id = $1
          AND lower(address) = lower($2)
        ORDER BY active_from_block_number
        ",
    )
    .bind(chain)
    .bind(DISCOVERED_RESOLVER)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        epochs,
        [
            (0, Some(100), false, None, Some("raw_log".into())),
            (101, None, true, None, Some("raw_log".into())),
        ]
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn partial_identity_redo_caps_a_binding_at_its_surviving_successor() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_binding_successor").await?;
    let chain = "interpret-binding-successor";
    seed_fixture(
        scratch.pool(),
        chain,
        &[(1, "alice"), (2, "alice"), (3, "alice")],
    )
    .await?;
    run_engine(scratch.pool(), chain, 0, 3, InterpretRunMode::Normal).await?;

    run_engine(scratch.pool(), chain, 2, 2, InterpretRunMode::Redo).await?;

    let epochs: Vec<(time::OffsetDateTime, Option<time::OffsetDateTime>)> = sqlx::query_as(
        "
        SELECT active_from, active_to
        FROM surface_bindings
        WHERE chain_id = $1
          AND canonicality_state IN ('canonical', 'safe', 'finalized')
        ORDER BY active_from
        ",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(epochs.len(), 3);
    assert_eq!(epochs[0].1, Some(epochs[1].0));
    assert_eq!(epochs[1].1, Some(epochs[2].0));
    assert!(epochs[2].1.is_none());
    scratch.cleanup().await
}

#[tokio::test]
async fn redo_reanchors_a_stable_binding_after_same_timestamp_predecessor_removal() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_binding_reanchor").await?;
    let chain = "interpret-binding-reanchor";
    seed_fixture_with_timestamps(
        scratch.pool(),
        chain,
        &[(1, "alice"), (2, "alice")],
        &[(1, 42), (2, 42)],
    )
    .await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;

    let (binding_id, old_start): (Uuid, time::OffsetDateTime) = sqlx::query_as(
        "SELECT surface_binding_id, active_from
         FROM surface_bindings
         WHERE chain_id = $1 AND block_number = 2",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE surface_bindings
         SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND block_number = 1",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;

    run_engine(scratch.pool(), chain, 2, 2, InterpretRunMode::Redo).await?;

    let (new_start, state): (time::OffsetDateTime, String) = sqlx::query_as(
        "SELECT active_from, canonicality_state::text
         FROM surface_bindings
         WHERE surface_binding_id = $1",
    )
    .bind(binding_id)
    .fetch_one(scratch.pool())
    .await?;
    assert!(new_start < old_start);
    assert_eq!(new_start.unix_timestamp(), 42);
    assert_eq!(state, "canonical");
    scratch.cleanup().await
}

#[tokio::test]
async fn discovery_redo_reopens_an_edge_when_the_replacement_range_omits_its_close() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_discovery_reopen").await?;
    let chain = "interpret-discovery-reopen";
    seed_discovery_fixture(scratch.pool(), chain).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    let block_two_hash = block_hash(chain, 2);
    sqlx::query(
        "
        INSERT INTO chain_lineage (
            chain_id, block_hash, parent_hash, block_number,
            block_timestamp, canonicality_state
        )
        VALUES ($1, $2, $3, 2, to_timestamp(2), 'canonical')
        ",
    )
    .bind(chain)
    .bind(&block_two_hash)
    .bind(block_hash(chain, 1))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "
        UPDATE discovery_edges
        SET active_to_block_number = 2,
            active_to_block_hash = $2,
            deactivated_at = now()
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .bind(&block_two_hash)
    .execute(scratch.pool())
    .await?;

    run_engine(scratch.pool(), chain, 1, 2, InterpretRunMode::Redo).await?;

    let edge: (String, bool, bool) = sqlx::query_as(
        "
        SELECT canonicality_state::text,
               active_to_block_number IS NULL,
               deactivated_at IS NULL
        FROM discovery_edges
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(edge, ("canonical".into(), true, true));
    scratch.cleanup().await
}

#[tokio::test]
async fn ens_v2_resource_identity_and_terminal_binding_round_trip() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_v2_lifecycle").await?;
    let chain = "interpret-v2-lifecycle";
    seed_v2_lifecycle_fixture(scratch.pool(), chain).await?;
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'safe' WHERE chain_id = $1 AND block_number = 1",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'finalized' WHERE chain_id = $1 AND block_number = 1",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;

    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;

    let resource_ids: Vec<Uuid> = sqlx::query_scalar(
        "
        SELECT DISTINCT resource_id
        FROM normalized_events
        WHERE chain_id = $1
          AND event_kind IN (
              'TokenResourceLinked',
              'PermissionChanged',
              'TokenControlTransferred'
          )
        ORDER BY resource_id
        ",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(resource_ids.len(), 1);
    let resource: (Uuid, Option<Uuid>, i64, String) = sqlx::query_as(
        "SELECT resource_id, token_lineage_id, block_number, canonicality_state::text \
         FROM resources WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(resource.0, resource_ids[0]);
    assert!(resource.1.is_some());
    assert_eq!((resource.2, resource.3.as_str()), (1, "finalized"));
    let lineage_anchor: (i64, String) = sqlx::query_as(
        "SELECT block_number, canonicality_state::text FROM token_lineages WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(lineage_anchor, (1, "finalized".into()));
    let binding: (bool, String) = sqlx::query_as(
        "
        SELECT active_to IS NOT NULL, canonicality_state::text
        FROM surface_bindings
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(binding, (true, "finalized".into()));
    let terminal_events: Vec<String> = sqlx::query_scalar(
        "
        SELECT event_kind
        FROM normalized_events
        WHERE chain_id = $1
          AND event_kind IN ('RegistrationReleased', 'SurfaceUnbound')
        ORDER BY event_kind
        ",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(terminal_events, ["RegistrationReleased", "SurfaceUnbound"]);
    scratch.cleanup().await
}

#[tokio::test]
async fn compatible_later_name_observation_preserves_the_finalized_first_anchor() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_surface_anchor").await?;
    let chain = "interpret-surface-anchor";
    seed_fixture(scratch.pool(), chain, &[(1, "alice"), (2, "alice")]).await?;
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'safe' WHERE chain_id = $1 AND block_number = 1",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'finalized' WHERE chain_id = $1 AND block_number = 1",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;

    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;

    let anchor: (i64, String, String) = sqlx::query_as(
        "SELECT block_number, block_hash, canonicality_state::text \
         FROM name_surfaces WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(anchor, (1, block_hash(chain, 1), "finalized".into()));
    scratch.cleanup().await
}

#[tokio::test]
async fn partial_redo_restores_resource_and_token_anchors_from_150_to_250() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_anchor_150_250").await?;
    let chain = "interpret-anchor-150-250";
    seed_v2_lifecycle_fixture(scratch.pool(), chain).await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;
    sqlx::query(
        "
        INSERT INTO chain_lineage (
            chain_id, block_hash, parent_hash, block_number, block_timestamp,
            canonicality_state
        )
        SELECT $1,
               $1 || '-block-' || height::text,
               $1 || '-block-' || (height - 1)::text,
               height,
               to_timestamp(height),
               'canonical'::canonicality_state
        FROM generate_series(3, 250) AS height
        ",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    let (resource_id, token_lineage_id): (Uuid, Uuid) =
        sqlx::query_as("SELECT resource_id, token_lineage_id FROM resources WHERE chain_id = $1")
            .bind(chain)
            .fetch_one(scratch.pool())
            .await?;
    sqlx::query(
        "
        INSERT INTO normalized_events (
            event_identity, namespace, logical_name_id, resource_id, event_kind,
            source_family, manifest_version, source_manifest_id, chain_id,
            block_number, block_hash, raw_fact_ref, derivation_kind,
            canonicality_state, before_state, after_state
        )
        SELECT 'anchor-observation-250', namespace, logical_name_id, $2,
               event_kind, source_family, manifest_version, source_manifest_id,
               chain_id, 250, $3, jsonb_build_object('kind', 'raw_block'),
               derivation_kind, 'canonical', '{}'::jsonb,
               jsonb_build_object('token_lineage_id', $4::text)
        FROM normalized_events
        WHERE chain_id = $1 AND resource_id = $2
        ORDER BY normalized_event_id
        LIMIT 1
        ",
    )
    .bind(chain)
    .bind(resource_id)
    .bind(block_hash(chain, 250))
    .bind(token_lineage_id)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "DELETE FROM normalized_events WHERE chain_id = $1 AND event_identity <> 'anchor-observation-250'",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    for table in ["name_surfaces", "resources", "token_lineages"] {
        sqlx::query(&format!(
            "UPDATE {table} SET block_number = 150, block_hash = $2, canonicality_state = 'canonical' WHERE chain_id = $1"
        ))
        .bind(chain)
        .bind(block_hash(chain, 150))
        .execute(scratch.pool())
        .await?;
    }
    sqlx::query(
        "
        UPDATE surface_bindings
        SET block_number = 150,
            block_hash = $2,
            active_to = NULL,
            canonicality_state = 'canonical'
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .bind(block_hash(chain, 150))
    .execute(scratch.pool())
    .await?;
    let transaction_hash = format!("{chain}-transaction-150");
    sqlx::query(
        "
        INSERT INTO raw_transactions (
            chain_id, block_hash, block_number, transaction_hash,
            transaction_index, from_address, to_address
        )
        VALUES ($1, $2, 150, $3, 0, $4, $5)
        ",
    )
    .bind(chain)
    .bind(block_hash(chain, 150))
    .bind(&transaction_hash)
    .bind(SENDER)
    .bind(CONTRACT)
    .execute(scratch.pool())
    .await?;
    let token_id = versioned_token("alice", 1);
    let owner: Address = "0x0000000000000000000000000000000000000061".parse()?;
    let registration = v2_registry_events::LabelRegistered {
        tokenId: token_id,
        labelHash: keccak256(b"alice"),
        label: "alice".to_owned(),
        owner,
        expiry: 1_000,
        sender: SENDER.parse()?,
    }
    .encode_log_data();
    insert_log_at(
        scratch.pool(),
        chain,
        150,
        &transaction_hash,
        0,
        CONTRACT,
        registration.topics(),
        registration.data.as_ref(),
    )
    .await?;
    let resource = v2_registry_events::TokenResource {
        tokenId: token_id,
        resource: U256::from(5001),
    }
    .encode_log_data();
    insert_log_at(
        scratch.pool(),
        chain,
        150,
        &transaction_hash,
        1,
        CONTRACT,
        resource.topics(),
        resource.data.as_ref(),
    )
    .await?;

    for redo_attempt in 1..=2 {
        run_engine(scratch.pool(), chain, 100, 200, InterpretRunMode::Redo).await?;

        for table in ["name_surfaces", "resources", "token_lineages"] {
            let anchor: (i64, String) = sqlx::query_as(&format!(
                "SELECT block_number, block_hash FROM {table} WHERE chain_id = $1"
            ))
            .bind(chain)
            .fetch_one(scratch.pool())
            .await?;
            assert_eq!(
                anchor,
                (250, block_hash(chain, 250)),
                "{table} after redo attempt {redo_attempt}"
            );
        }
    }
    scratch.cleanup().await
}

#[tokio::test]
async fn redo_without_the_orphaned_terminal_fact_reopens_the_prior_binding() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_terminal_redo").await?;
    let chain = "interpret-terminal-redo";
    seed_v2_lifecycle_fixture(scratch.pool(), chain).await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;
    sqlx::query(
        "
        UPDATE chain_lineage
        SET canonicality_state = 'orphaned'
        WHERE chain_id = $1
          AND block_number = 2
        ",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    let winning_hash = format!("{chain}-winning-2");
    sqlx::query(
        "
        INSERT INTO chain_lineage (
            chain_id, block_hash, parent_hash, block_number,
            block_timestamp, canonicality_state
        )
        VALUES ($1, $2, $3, 2, to_timestamp(2), 'canonical')
        ",
    )
    .bind(chain)
    .bind(&winning_hash)
    .bind(block_hash(chain, 1))
    .execute(scratch.pool())
    .await?;

    run_engine(scratch.pool(), chain, 2, 2, InterpretRunMode::Redo).await?;

    let open: bool =
        sqlx::query_scalar("SELECT active_to IS NULL FROM surface_bindings WHERE chain_id = $1")
            .bind(chain)
            .fetch_one(scratch.pool())
            .await?;
    assert!(open);
    let terminal_count: i64 = sqlx::query_scalar(
        "
        SELECT count(*)
        FROM normalized_events
        WHERE chain_id = $1
          AND event_kind IN ('RegistrationReleased', 'SurfaceUnbound')
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(terminal_count, 0);
    scratch.cleanup().await
}

#[tokio::test]
async fn binding_open_redo_preserves_a_terminal_close_after_the_range() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_binding_terminal").await?;
    let chain = "interpret-binding-terminal";
    seed_v2_lifecycle_fixture(scratch.pool(), chain).await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;
    let before: time::OffsetDateTime =
        sqlx::query_scalar("SELECT active_to FROM surface_bindings WHERE chain_id = $1")
            .bind(chain)
            .fetch_one(scratch.pool())
            .await?;

    run_engine(scratch.pool(), chain, 1, 1, InterpretRunMode::Redo).await?;

    let after: time::OffsetDateTime =
        sqlx::query_scalar("SELECT active_to FROM surface_bindings WHERE chain_id = $1")
            .bind(chain)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(after, before);
    let terminal_events: i64 = sqlx::query_scalar(
        "
        SELECT count(*)
        FROM normalized_events
        WHERE chain_id = $1
          AND block_number = 2
          AND event_kind IN ('RegistrationReleased', 'SurfaceUnbound')
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(terminal_events, 2);
    scratch.cleanup().await
}

#[tokio::test]
async fn interpret_ignores_raw_facts_on_orphaned_lineage() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_canonical").await?;
    let chain = "interpret-canonical";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    insert_orphaned_registration(scratch.pool(), chain, 1, "bob").await?;

    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    let registrations: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events WHERE event_kind = 'RegistrationGranted'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(registrations, 1);
    let orphan_preimage: i64 =
        sqlx::query_scalar("SELECT count(*) FROM label_preimages WHERE decoded_label = 'bob'")
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(orphan_preimage, 0);
    scratch.cleanup().await
}

#[tokio::test]
async fn mid_batch_orphaning_aborts_the_write_as_transient() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_mid_batch_orphan").await?;
    let chain = "interpret-mid-batch-orphan";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    let mut reorg = scratch.pool().begin().await?;
    sqlx::query(
        "
        SELECT block_hash
        FROM chain_lineage
        WHERE chain_id = $1 AND block_number = 1
        FOR UPDATE
        ",
    )
    .bind(chain)
    .fetch_one(&mut *reorg)
    .await?;

    let pool = scratch.pool().clone();
    let chain_for_task = chain.to_owned();
    let running = tokio::spawn(async move {
        Engine::new(pool)
            .run_batch(BatchRequest {
                chain_id: chain_for_task,
                from_block: 0,
                to_block: 1,
                resume_current: None,
                mode: InterpretRunMode::Normal,
            })
            .await
    });
    let mut reached_write_revalidation = false;
    for _ in 0..200 {
        reached_write_revalidation = sqlx::query_scalar(
            "
            SELECT EXISTS (
                SELECT 1
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND wait_event_type = 'Lock'
                  AND query LIKE '%FOR SHARE%'
            )
            ",
        )
        .fetch_one(scratch.pool())
        .await?;
        if reached_write_revalidation {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        reached_write_revalidation,
        "interpretation must reach write-time lineage revalidation"
    );
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'orphaned' WHERE chain_id = $1 AND block_number = 1",
    )
    .bind(chain)
    .execute(&mut *reorg)
    .await?;
    reorg.commit().await?;

    let error = running
        .await?
        .expect_err("an orphaned loaded batch must be retried");
    assert_eq!(error.kind(), InterpretErrorKind::Transient);
    assert!(error.to_string().contains("lineage changed before write"));
    let written: i64 = sqlx::query_scalar("SELECT count(*) FROM normalized_events")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(written, 0);
    scratch.cleanup().await
}

#[tokio::test]
async fn reorg_between_block_and_log_loads_is_transient_and_retries() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_load_snapshot_reorg").await?;
    let chain = "interpret-load-snapshot-reorg";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    insert_orphaned_registration(scratch.pool(), chain, 1, "bob").await?;

    let mut raw_log_lock = scratch.pool().begin().await?;
    sqlx::query("LOCK TABLE raw_logs IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *raw_log_lock)
        .await?;
    let pool = scratch.pool().clone();
    let chain_for_task = chain.to_owned();
    let running = tokio::spawn(async move {
        Engine::new(pool)
            .run_batch(BatchRequest {
                chain_id: chain_for_task,
                from_block: 0,
                to_block: 1,
                resume_current: None,
                mode: InterpretRunMode::Normal,
            })
            .await
    });

    let mut waiting_for_raw_logs = false;
    for _ in 0..200 {
        waiting_for_raw_logs = sqlx::query_scalar(
            "
            SELECT EXISTS (
                SELECT 1
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND pid <> pg_backend_pid()
                  AND wait_event_type = 'Lock'
                  AND query LIKE '%FROM raw_logs raw%'
            )
            ",
        )
        .fetch_one(scratch.pool())
        .await?;
        if waiting_for_raw_logs {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        waiting_for_raw_logs,
        "interpretation must reach the raw-log load"
    );

    sqlx::query(
        "
        UPDATE chain_lineage
        SET canonicality_state = CASE
            WHEN block_hash = $2 THEN 'canonical'::canonicality_state
            ELSE 'orphaned'::canonicality_state
        END
        WHERE chain_id = $1 AND block_number = 1
        ",
    )
    .bind(chain)
    .bind(format!("{chain}-orphan-1"))
    .execute(scratch.pool())
    .await?;
    raw_log_lock.commit().await?;

    let error = running
        .await?
        .expect_err("lineage churn during load must retry");
    assert_eq!(error.kind(), InterpretErrorKind::Transient);
    let written: i64 = sqlx::query_scalar("SELECT count(*) FROM normalized_events")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(written, 0);

    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    let labels: Vec<String> = sqlx::query_scalar(
        "SELECT decoded_label FROM label_preimages WHERE decoded_label IS NOT NULL ORDER BY decoded_label",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert!(labels.iter().any(|label| label == "bob"));
    assert!(!labels.iter().any(|label| label == "alice"));
    scratch.cleanup().await
}

#[tokio::test]
async fn aba_reorg_between_marker_selection_and_snapshot_load_is_transient() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_load_snapshot_aba").await?;
    let chain = "interpret-load-snapshot-aba";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    insert_orphaned_registration(scratch.pool(), chain, 1, "bob").await?;

    let (engine_pool, batch_acquire_reached, release_batch_acquire) =
        pool_with_acquire_gate(scratch.pool(), 3).await?;

    let mut raw_log_gate = scratch.pool().begin().await?;
    sqlx::query("LOCK TABLE raw_logs IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *raw_log_gate)
        .await?;
    let pool = engine_pool.clone();
    let chain_for_task = chain.to_owned();
    let running = tokio::spawn(async move {
        Engine::new(pool)
            .run_batch(BatchRequest {
                chain_id: chain_for_task,
                from_block: 0,
                to_block: 1,
                resume_current: None,
                mode: InterpretRunMode::Normal,
            })
            .await
    });

    batch_acquire_reached.notified().await;
    switch_live_lineage(scratch.pool(), chain, &format!("{chain}-orphan-1")).await?;
    release_batch_acquire.notify_one();

    wait_for_locked_query(scratch.pool(), "%FROM raw_logs raw%").await?;
    switch_live_lineage(scratch.pool(), chain, &block_hash(chain, 1)).await?;
    raw_log_gate.commit().await?;

    let error = running
        .await?
        .expect_err("an ABA lineage change must not mix selected and loaded blocks");
    assert_eq!(error.kind(), InterpretErrorKind::Transient);
    assert!(
        error
            .to_string()
            .contains("changed while loading raw facts")
    );
    let written: i64 = sqlx::query_scalar("SELECT count(*) FROM normalized_events")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(written, 0);
    engine_pool.close().await;
    scratch.cleanup().await
}

#[tokio::test]
async fn changed_resume_marker_before_snapshot_is_transient_without_writes() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_resume_snapshot_reorg").await?;
    let chain = "interpret-resume-snapshot-reorg";
    seed_fixture(scratch.pool(), chain, &[(1, "alice"), (2, "alice")]).await?;
    insert_orphaned_registration(scratch.pool(), chain, 1, "bob").await?;
    insert_orphaned_registration_with_parent(
        scratch.pool(),
        chain,
        2,
        "bob",
        &format!("{chain}-orphan-1"),
    )
    .await?;

    let (engine_pool, suffix_selection_reached, release_suffix_selection) =
        pool_with_acquire_gate(scratch.pool(), 3).await?;
    let pool = engine_pool.clone();
    let chain_for_task = chain.to_owned();
    let running = tokio::spawn(async move {
        Engine::new(pool)
            .run_batch(BatchRequest {
                chain_id: chain_for_task,
                from_block: 0,
                to_block: 2,
                resume_current: Some(Marker {
                    number: 1,
                    hash: block_hash(chain, 1),
                }),
                mode: InterpretRunMode::Normal,
            })
            .await
    });

    suffix_selection_reached.notified().await;
    let mut reorg = scratch.pool().begin().await?;
    sqlx::query(
        "
        UPDATE chain_lineage
        SET canonicality_state = 'orphaned'::canonicality_state
        WHERE chain_id = $1 AND block_number BETWEEN 1 AND 2
        ",
    )
    .bind(chain)
    .execute(&mut *reorg)
    .await?;
    sqlx::query(
        "
        UPDATE chain_lineage
        SET canonicality_state = 'canonical'::canonicality_state
        WHERE chain_id = $1
          AND block_hash IN ($2, $3)
        ",
    )
    .bind(chain)
    .bind(format!("{chain}-orphan-1"))
    .bind(format!("{chain}-orphan-2"))
    .execute(&mut *reorg)
    .await?;
    reorg.commit().await?;
    release_suffix_selection.notify_one();

    let error = running
        .await?
        .expect_err("a replaced resume block must abort the resumed suffix");
    assert_eq!(error.kind(), InterpretErrorKind::Transient);
    assert!(
        error
            .to_string()
            .contains("changed before the input snapshot")
    );
    let written: i64 = sqlx::query_scalar("SELECT count(*) FROM normalized_events")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(written, 0);
    engine_pool.close().await;
    scratch.cleanup().await
}

#[tokio::test]
async fn prior_state_ignores_events_whose_lineage_is_no_longer_live() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_prior_live_lineage").await?;
    let chain = "interpret-prior-live-lineage";
    seed_fixture(scratch.pool(), chain, &[(1, "alice"), (2, "alice")]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    insert_orphaned_registration(scratch.pool(), chain, 1, "alice").await?;
    sqlx::query(
        "
        UPDATE chain_lineage
        SET canonicality_state = CASE
            WHEN block_hash = $2 THEN 'canonical'::canonicality_state
            ELSE 'orphaned'::canonicality_state
        END
        WHERE chain_id = $1 AND block_number = 1
        ",
    )
    .bind(chain)
    .bind(format!("{chain}-orphan-1"))
    .execute(scratch.pool())
    .await?;

    run_engine(scratch.pool(), chain, 2, 2, InterpretRunMode::Normal).await?;
    let prior_registrant: Option<String> = sqlx::query_scalar(
        "
        SELECT before_state ->> 'registrant'
        FROM normalized_events
        WHERE chain_id = $1
          AND block_number = 2
          AND event_kind = 'RegistrationGranted'
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(prior_registrant, None);
    scratch.cleanup().await
}

#[tokio::test]
async fn orphaned_token_and_resource_identities_reanchor_to_the_winning_observation() -> Result<()>
{
    let scratch = ScratchDatabase::create("production_interpret_identity_reanchor").await?;
    let chain = "interpret-identity-reanchor";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    sqlx::query(
        "
        UPDATE resources
        SET block_hash = $2,
            block_number = 0,
            provenance = '{\"branch\":\"losing\"}'::jsonb,
            canonicality_state = 'orphaned'
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .bind(block_hash(chain, 0))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "
        UPDATE token_lineages
        SET block_hash = $2,
            block_number = 0,
            provenance = '{\"branch\":\"losing\"}'::jsonb,
            canonicality_state = 'orphaned'
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .bind(block_hash(chain, 0))
    .execute(scratch.pool())
    .await?;

    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    let resource_anchor: (i64, bool, String) = sqlx::query_as(
        "
        SELECT block_number,
               provenance ->> 'branch' IS NULL,
               canonicality_state::text
        FROM resources
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    let lineage_anchor: (i64, bool, String) = sqlx::query_as(
        "
        SELECT block_number,
               provenance ->> 'branch' IS NULL,
               canonicality_state::text
        FROM token_lineages
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(resource_anchor, (1, true, "canonical".into()));
    assert_eq!(lineage_anchor, (1, true, "canonical".into()));
    scratch.cleanup().await
}

#[tokio::test]
async fn readable_closed_binding_is_not_reopened_by_idempotent_replay() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_closed_binding").await?;
    let chain = "interpret-closed-binding";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    sqlx::query(
        "
        UPDATE surface_bindings
        SET active_to = active_from + interval '1 second'
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;

    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    let remains_closed: bool = sqlx::query_scalar(
        "SELECT active_to IS NOT NULL FROM surface_bindings WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert!(remains_closed);
    scratch.cleanup().await
}

async fn run_engine(
    pool: &PgPool,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
    mode: InterpretRunMode,
) -> Result<()> {
    let outcome = Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: chain_id.to_owned(),
            from_block,
            to_block,
            resume_current: None,
            mode,
        })
        .await?;
    assert!(outcome.complete);
    assert_eq!(outcome.current, outcome.target);
    Ok(())
}

async fn seed_discovery_fixture(pool: &PgPool, chain_id: &str) -> Result<()> {
    for block in 0..=1 {
        sqlx::query(
            "
            INSERT INTO chain_lineage (
                chain_id, block_hash, parent_hash, block_number, block_timestamp,
                canonicality_state
            )
            VALUES ($1, $2, $3, $4, to_timestamp($4), 'canonical')
            ",
        )
        .bind(chain_id)
        .bind(block_hash(chain_id, block))
        .bind((block > 0).then(|| block_hash(chain_id, block - 1)))
        .bind(block)
        .execute(pool)
        .await?;
    }
    let registry_instance_id = Uuid::new_v4();
    sqlx::query("INSERT INTO contract_instances VALUES ($1, $2, 'contract', '{}'::jsonb, now())")
        .bind(registry_instance_id)
        .bind(chain_id)
        .execute(pool)
        .await?;
    let registry_payload = json!({
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
            "edge_kind": "resolver",
            "from_role": "registry",
            "admission": "reachable_from_root"
        }],
        "abi": { "events": [{
            "name": "ResolverUpdated",
            "fragment": "event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender)",
            "emitter_roles": ["registry"],
            "normalized_events": ["ResolverChanged"]
        }], "calls": [] }
    });
    let resolver_payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v2_resolver_l1",
        "chain": chain_id,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [],
        "discovery_rules": [],
        "abi": { "events": [{
            "name": "TextChanged",
            "fragment": "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
            "emitter_roles": [],
            "normalized_events": ["RecordChanged"]
        }], "calls": [] }
    });
    let registry_manifest_id = insert_manifest(
        pool,
        chain_id,
        "ens_v2_registry_l1",
        "tests/discovery-registry.toml",
        registry_payload,
    )
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_discovery_rules (
            manifest_id, edge_kind, from_role, admission
        )
        VALUES ($1, 'resolver', 'registry', 'reachable_from_root')
        ",
    )
    .bind(registry_manifest_id)
    .execute(pool)
    .await?;
    insert_manifest(
        pool,
        chain_id,
        "ens_v2_resolver_l1",
        "tests/discovery-resolver.toml",
        resolver_payload,
    )
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_contract_instances (
            manifest_id, chain_id, declaration_kind, declaration_name,
            contract_instance_id, declared_address, role, proxy_kind, start_block_number
        )
        VALUES ($1, $2, 'contract', 'registry', $3, $4, 'registry', 'none', 0)
        ",
    )
    .bind(registry_manifest_id)
    .bind(chain_id)
    .bind(registry_instance_id)
    .bind(CONTRACT)
    .execute(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO contract_instance_addresses (
            contract_instance_id, chain_id, address, active_from_block_number,
            source_manifest_id, provenance
        )
        VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)
        ",
    )
    .bind(registry_instance_id)
    .bind(chain_id)
    .bind(CONTRACT)
    .bind(registry_manifest_id)
    .execute(pool)
    .await?;

    let transaction_hash = format!("{chain_id}-transaction-1");
    sqlx::query(
        "
        INSERT INTO raw_transactions (
            chain_id, block_hash, block_number, transaction_hash, transaction_index,
            from_address, to_address
        )
        VALUES ($1, $2, 1, $3, 0, $4, $5)
        ",
    )
    .bind(chain_id)
    .bind(block_hash(chain_id, 1))
    .bind(&transaction_hash)
    .bind(SENDER)
    .bind(CONTRACT)
    .execute(pool)
    .await?;
    let resolver_address = DISCOVERED_RESOLVER.parse::<Address>()?;
    let discovery = ResolverUpdated {
        tokenId: U256::from(7),
        resolver: resolver_address,
        sender: SENDER.parse::<Address>()?,
    }
    .encode_log_data();
    insert_log(
        pool,
        chain_id,
        &transaction_hash,
        0,
        CONTRACT,
        discovery.topics(),
        discovery.data.as_ref(),
    )
    .await?;
    let record = TextChanged {
        node: B256::from(keccak256(b"alice.eth")),
        indexedKey: keccak256(b"url"),
        key: "url".into(),
        value: "https://example.test".into(),
    }
    .encode_log_data();
    insert_log(
        pool,
        chain_id,
        &transaction_hash,
        1,
        DISCOVERED_RESOLVER,
        record.topics(),
        record.data.as_ref(),
    )
    .await
}

async fn seed_root_resolver_watch_fixture(pool: &PgPool, chain_id: &str) -> Result<()> {
    let root_id = Uuid::new_v4();
    let resolver_id = Uuid::new_v4();
    for instance_id in [root_id, resolver_id] {
        sqlx::query(
            "
            INSERT INTO contract_instances (
                contract_instance_id, chain_id, contract_kind, provenance
            )
            VALUES ($1, $2, 'contract', '{}'::jsonb)
            ",
        )
        .bind(instance_id)
        .bind(chain_id)
        .execute(pool)
        .await?;
    }
    let root_payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v2_root_l1",
        "chain": chain_id,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [{
            "role": "root_registry",
            "address": CONTRACT,
            "proxy_kind": "none",
            "implementation": null,
            "start_block": 0
        }],
        "discovery_rules": [{
            "edge_kind": "resolver",
            "from_role": "root_registry",
            "admission": "reachable_from_root"
        }],
        "abi": { "events": [{
            "name": "ResolverUpdated",
            "fragment": "event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender)",
            "emitter_roles": ["root_registry"],
            "normalized_events": ["ResolverChanged"]
        }], "calls": [] }
    });
    let resolver_payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v2_resolver_l1",
        "chain": chain_id,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [],
        "discovery_rules": [],
        "abi": { "events": [{
            "name": "TextChanged",
            "fragment": "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
            "emitter_roles": [],
            "normalized_events": ["RecordChanged"]
        }], "calls": [] }
    });
    let root_manifest_id = insert_manifest(
        pool,
        chain_id,
        "ens_v2_root_l1",
        "tests/root-resolver-root.toml",
        root_payload,
    )
    .await?;
    insert_manifest(
        pool,
        chain_id,
        "ens_v2_resolver_l1",
        "tests/root-resolver-target.toml",
        resolver_payload,
    )
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_contract_instances (
            manifest_id, chain_id, declaration_kind, declaration_name,
            contract_instance_id, declared_address, role, proxy_kind,
            start_block_number
        )
        VALUES ($1, $2, 'contract', 'root_registry', $3, $4,
                'root_registry', 'none', 0)
        ",
    )
    .bind(root_manifest_id)
    .bind(chain_id)
    .bind(root_id)
    .bind(CONTRACT)
    .execute(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_discovery_rules (
            manifest_id, edge_kind, from_role, admission
        )
        VALUES ($1, 'resolver', 'root_registry', 'reachable_from_root')
        ",
    )
    .bind(root_manifest_id)
    .execute(pool)
    .await?;
    for (instance_id, address, active_from) in [
        (root_id, CONTRACT, 0_i64),
        (resolver_id, DISCOVERED_RESOLVER, 1_i64),
    ] {
        sqlx::query(
            "
            INSERT INTO contract_instance_addresses (
                contract_instance_id, chain_id, address,
                active_from_block_number, source_manifest_id, provenance
            )
            VALUES ($1, $2, $3, $4, $5, '{}'::jsonb)
            ",
        )
        .bind(instance_id)
        .bind(chain_id)
        .bind(address)
        .bind(active_from)
        .bind(root_manifest_id)
        .execute(pool)
        .await?;
    }
    sqlx::query(
        "
        INSERT INTO discovery_edges (
            chain_id, edge_kind, from_contract_instance_id,
            to_contract_instance_id, discovery_source, admission_basis,
            source_manifest_id, active_from_block_number,
            active_from_block_hash, canonicality_state, provenance
        )
        VALUES ($1, 'resolver', $2, $3, 'ResolverUpdated',
                'reachable_from_root', $4, 1, $5, 'canonical', '{}'::jsonb)
        ",
    )
    .bind(chain_id)
    .bind(root_id)
    .bind(resolver_id)
    .bind(root_manifest_id)
    .bind(format!("{chain_id}-block-1"))
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_sibling_discovery_fixture(pool: &PgPool, chain_id: &str) -> Result<()> {
    for block in 0..=2 {
        sqlx::query(
            "
            INSERT INTO chain_lineage (
                chain_id, block_hash, parent_hash, block_number, block_timestamp,
                canonicality_state
            )
            VALUES ($1, $2, $3, $4, to_timestamp($4), 'canonical')
            ",
        )
        .bind(chain_id)
        .bind(block_hash(chain_id, block))
        .bind((block > 0).then(|| block_hash(chain_id, block - 1)))
        .bind(block)
        .execute(pool)
        .await?;
    }
    let registry_instance_id = Uuid::new_v4();
    sqlx::query("INSERT INTO contract_instances VALUES ($1, $2, 'contract', '{}'::jsonb, now())")
        .bind(registry_instance_id)
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
            "edge_kind": "resolver",
            "from_role": "registry",
            "admission": "reachable_from_root"
        }],
        "abi": { "events": [{
            "name": "ResolverUpdated",
            "fragment": "event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender)",
            "emitter_roles": ["registry"],
            "normalized_events": ["ResolverChanged"]
        }], "calls": [] }
    });
    let manifest_id = insert_manifest(
        pool,
        chain_id,
        "ens_v2_registry_l1",
        "tests/discovery-siblings.toml",
        payload,
    )
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_discovery_rules (
            manifest_id, edge_kind, from_role, admission
        )
        VALUES ($1, 'resolver', 'registry', 'reachable_from_root')
        ",
    )
    .bind(manifest_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_contract_instances (
            manifest_id, chain_id, declaration_kind, declaration_name,
            contract_instance_id, declared_address, role, proxy_kind,
            start_block_number
        )
        VALUES ($1, $2, 'contract', 'registry', $3, $4, 'registry', 'none', 0)
        ",
    )
    .bind(manifest_id)
    .bind(chain_id)
    .bind(registry_instance_id)
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
    .bind(registry_instance_id)
    .bind(chain_id)
    .bind(CONTRACT)
    .bind(manifest_id)
    .execute(pool)
    .await?;

    for block in 1..=2 {
        sqlx::query(
            "
            INSERT INTO raw_transactions (
                chain_id, block_hash, block_number, transaction_hash,
                transaction_index, from_address, to_address
            )
            VALUES ($1, $2, $3, $4, 0, $5, $6)
            ",
        )
        .bind(chain_id)
        .bind(block_hash(chain_id, block))
        .bind(block)
        .bind(format!("{chain_id}-transaction-{block}"))
        .bind(SENDER)
        .bind(CONTRACT)
        .execute(pool)
        .await?;
    }
    let token_a = versioned_token("alice", 1);
    let token_b = versioned_token("bob", 1);
    for (block, log_index, token_id, target) in [
        (1, 0, token_a, "0x0000000000000000000000000000000000000051"),
        (1, 1, token_b, "0x0000000000000000000000000000000000000052"),
        (
            2,
            0,
            versioned_token("alice", 2),
            "0x0000000000000000000000000000000000000053",
        ),
    ] {
        let event = ResolverUpdated {
            tokenId: token_id,
            resolver: target.parse()?,
            sender: SENDER.parse()?,
        }
        .encode_log_data();
        insert_log_at(
            pool,
            chain_id,
            block,
            &format!("{chain_id}-transaction-{block}"),
            log_index,
            CONTRACT,
            event.topics(),
            event.data.as_ref(),
        )
        .await?;
    }
    Ok(())
}

async fn seed_v2_lifecycle_fixture(pool: &PgPool, chain_id: &str) -> Result<()> {
    for block in 0..=2 {
        sqlx::query(
            "
            INSERT INTO chain_lineage (
                chain_id, block_hash, parent_hash, block_number, block_timestamp,
                canonicality_state
            )
            VALUES ($1, $2, $3, $4, to_timestamp($4), 'canonical')
            ",
        )
        .bind(chain_id)
        .bind(block_hash(chain_id, block))
        .bind((block > 0).then(|| block_hash(chain_id, block - 1)))
        .bind(block)
        .execute(pool)
        .await?;
    }
    let registry_instance_id = Uuid::new_v4();
    sqlx::query("INSERT INTO contract_instances VALUES ($1, $2, 'contract', '{}'::jsonb, now())")
        .bind(registry_instance_id)
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
        "discovery_rules": [],
        "abi": { "events": [
            {
                "name": "LabelRegistered",
                "fragment": "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                "emitter_roles": ["registry"],
                "normalized_events": ["RegistrationGranted"]
            },
            {
                "name": "TokenResource",
                "fragment": "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                "emitter_roles": ["registry"],
                "normalized_events": ["TokenResourceLinked"]
            },
            {
                "name": "EACRolesChanged",
                "fragment": "event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap)",
                "emitter_roles": ["registry"],
                "normalized_events": ["PermissionChanged", "RootPermissionChanged"]
            },
            {
                "name": "TransferSingle",
                "fragment": "event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)",
                "emitter_roles": ["registry"],
                "normalized_events": ["TokenControlTransferred"]
            },
            {
                "name": "LabelUnregistered",
                "fragment": "event LabelUnregistered(uint256 indexed tokenId, address indexed sender)",
                "emitter_roles": ["registry"],
                "normalized_events": ["RegistrationReleased"]
            }
        ], "calls": [] }
    });
    let manifest_id = insert_manifest(
        pool,
        chain_id,
        "ens_v2_registry_l1",
        "tests/v2-lifecycle.toml",
        payload,
    )
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_contract_instances (
            manifest_id, chain_id, declaration_kind, declaration_name,
            contract_instance_id, declared_address, role, proxy_kind,
            start_block_number
        )
        VALUES ($1, $2, 'contract', 'registry', $3, $4, 'registry', 'none', 0)
        ",
    )
    .bind(manifest_id)
    .bind(chain_id)
    .bind(registry_instance_id)
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
    .bind(registry_instance_id)
    .bind(chain_id)
    .bind(CONTRACT)
    .bind(manifest_id)
    .execute(pool)
    .await?;
    for block in 1..=2 {
        sqlx::query(
            "
            INSERT INTO raw_transactions (
                chain_id, block_hash, block_number, transaction_hash,
                transaction_index, from_address, to_address
            )
            VALUES ($1, $2, $3, $4, 0, $5, $6)
            ",
        )
        .bind(chain_id)
        .bind(block_hash(chain_id, block))
        .bind(block)
        .bind(format!("{chain_id}-transaction-{block}"))
        .bind(SENDER)
        .bind(CONTRACT)
        .execute(pool)
        .await?;
    }
    let label = "alice";
    let token_id = versioned_token(label, 1);
    let resource_id = U256::from(5001);
    let owner: Address = "0x0000000000000000000000000000000000000061".parse()?;
    let recipient: Address = "0x0000000000000000000000000000000000000062".parse()?;
    let block_one = format!("{chain_id}-transaction-1");
    let facts = [
        v2_registry_events::LabelRegistered {
            tokenId: token_id,
            labelHash: keccak256(label.as_bytes()),
            label: label.to_owned(),
            owner,
            expiry: 100,
            sender: SENDER.parse()?,
        }
        .encode_log_data(),
        v2_registry_events::TokenResource {
            tokenId: token_id,
            resource: resource_id,
        }
        .encode_log_data(),
        v2_registry_events::EACRolesChanged {
            resource: resource_id,
            account: owner,
            oldRoleBitmap: U256::ZERO,
            newRoleBitmap: U256::from(1),
        }
        .encode_log_data(),
        v2_registry_events::TransferSingle {
            operator: owner,
            from: owner,
            to: recipient,
            id: token_id,
            value: U256::from(1),
        }
        .encode_log_data(),
    ];
    for (log_index, fact) in facts.into_iter().enumerate() {
        insert_log_at(
            pool,
            chain_id,
            1,
            &block_one,
            i64::try_from(log_index)?,
            CONTRACT,
            fact.topics(),
            fact.data.as_ref(),
        )
        .await?;
    }
    let release = v2_registry_events::LabelUnregistered {
        tokenId: token_id,
        sender: SENDER.parse()?,
    }
    .encode_log_data();
    insert_log_at(
        pool,
        chain_id,
        2,
        &format!("{chain_id}-transaction-2"),
        0,
        CONTRACT,
        release.topics(),
        release.data.as_ref(),
    )
    .await
}

async fn seed_announcement_fixture(pool: &PgPool, chain_id: &str) -> Result<()> {
    for block in 0..=1 {
        sqlx::query(
            "
            INSERT INTO chain_lineage (
                chain_id, block_hash, parent_hash, block_number, block_timestamp,
                canonicality_state
            )
            VALUES ($1, $2, $3, $4, to_timestamp($4), 'canonical')
            ",
        )
        .bind(chain_id)
        .bind(block_hash(chain_id, block))
        .bind((block > 0).then(|| block_hash(chain_id, block - 1)))
        .bind(block)
        .execute(pool)
        .await?;
    }
    let anchor_instance_id = Uuid::new_v4();
    sqlx::query("INSERT INTO contract_instances VALUES ($1, $2, 'contract', '{}'::jsonb, now())")
        .bind(anchor_instance_id)
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
    let manifest_id = insert_manifest(
        pool,
        chain_id,
        "ens_v2_registry_l1",
        "tests/announcement-registry.toml",
        payload,
    )
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_contract_instances (
            manifest_id, chain_id, declaration_kind, declaration_name,
            contract_instance_id, declared_address, role, proxy_kind,
            start_block_number
        )
        VALUES ($1, $2, 'contract', 'registry', $3, $4, 'registry', 'none', 0)
        ",
    )
    .bind(manifest_id)
    .bind(chain_id)
    .bind(anchor_instance_id)
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
    .bind(anchor_instance_id)
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

    let transaction_hash = format!("{chain_id}-transaction-1");
    sqlx::query(
        "
        INSERT INTO raw_transactions (
            chain_id, block_hash, block_number, transaction_hash,
            transaction_index, from_address, to_address
        )
        VALUES ($1, $2, 1, $3, 0, $4, $5)
        ",
    )
    .bind(chain_id)
    .bind(block_hash(chain_id, 1))
    .bind(&transaction_hash)
    .bind(SENDER)
    .bind(ANNOUNCED_REGISTRY)
    .execute(pool)
    .await?;
    let announcement = RegistryCreated {}.encode_log_data();
    insert_log(
        pool,
        chain_id,
        &transaction_hash,
        0,
        ANNOUNCED_REGISTRY,
        announcement.topics(),
        announcement.data.as_ref(),
    )
    .await
}

async fn seed_hostile_wrapper_fixture(
    pool: &PgPool,
    chain_id: &str,
    raw_label: &[u8],
) -> Result<()> {
    seed_fixture(pool, chain_id, &[]).await?;
    sqlx::query(
        "
        INSERT INTO chain_lineage (
            chain_id, block_hash, parent_hash, block_number, block_timestamp,
            canonicality_state
        )
        VALUES ($1, $2, $3, 1, to_timestamp(1), 'canonical')
        ",
    )
    .bind(chain_id)
    .bind(block_hash(chain_id, 1))
    .bind(block_hash(chain_id, 0))
    .execute(pool)
    .await?;
    let payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v1_wrapper_l1",
        "chain": chain_id,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [{
            "role": "name_wrapper",
            "address": ANNOUNCED_REGISTRY,
            "proxy_kind": "none",
            "implementation": null,
            "start_block": 0
        }],
        "discovery_rules": [],
        "abi": { "events": [{
            "name": "NameWrapped",
            "fragment": "event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry)",
            "emitter_roles": ["name_wrapper"],
            "normalized_events": [
                "TokenControlTransferred",
                "ExpiryChanged",
                "PermissionScopeChanged",
                "SurfaceBound",
                "AuthorityEpochChanged",
                "PreimageObserved"
            ]
        }], "calls": [] }
    });
    let manifest_id = insert_manifest(
        pool,
        chain_id,
        "ens_v1_wrapper_l1",
        "tests/hostile-wrapper.toml",
        payload,
    )
    .await?;
    let instance_id = Uuid::new_v4();
    sqlx::query("INSERT INTO contract_instances VALUES ($1, $2, 'contract', '{}'::jsonb, now())")
        .bind(instance_id)
        .bind(chain_id)
        .execute(pool)
        .await?;
    sqlx::query(
        "
        INSERT INTO manifest_contract_instances (
            manifest_id, chain_id, declaration_kind, declaration_name,
            contract_instance_id, declared_address, role, proxy_kind,
            start_block_number
        )
        VALUES ($1, $2, 'contract', 'name_wrapper', $3, $4,
                'name_wrapper', 'none', 0)
        ",
    )
    .bind(manifest_id)
    .bind(chain_id)
    .bind(instance_id)
    .bind(ANNOUNCED_REGISTRY)
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
    .bind(instance_id)
    .bind(chain_id)
    .bind(ANNOUNCED_REGISTRY)
    .bind(manifest_id)
    .execute(pool)
    .await?;
    let mut dns_name = Vec::with_capacity(raw_label.len() + 6);
    dns_name.push(u8::try_from(raw_label.len())?);
    dns_name.extend_from_slice(raw_label);
    dns_name.extend_from_slice(b"\x03eth\0");
    let node = raw_namehash(&[raw_label, b"eth"]);
    let encoded = NameWrapped {
        node,
        name: dns_name.into(),
        owner: CONTRACT.parse()?,
        fuses: 1,
        expiry: 42,
    }
    .encode_log_data();
    let transaction_hash = format!("{chain_id}-transaction-1");
    sqlx::query(
        "
        INSERT INTO raw_transactions (
            chain_id, block_hash, block_number, transaction_hash,
            transaction_index, from_address, to_address
        )
        VALUES ($1, $2, 1, $3, 0, $4, $5)
        ",
    )
    .bind(chain_id)
    .bind(block_hash(chain_id, 1))
    .bind(&transaction_hash)
    .bind(SENDER)
    .bind(ANNOUNCED_REGISTRY)
    .execute(pool)
    .await?;
    insert_log(
        pool,
        chain_id,
        &transaction_hash,
        0,
        ANNOUNCED_REGISTRY,
        encoded.topics(),
        encoded.data.as_ref(),
    )
    .await
}

fn raw_namehash(labels: &[&[u8]]) -> B256 {
    labels.iter().rev().fold(B256::ZERO, |node, label| {
        let mut input = [0_u8; 64];
        input[..32].copy_from_slice(node.as_slice());
        input[32..].copy_from_slice(keccak256(label).as_slice());
        B256::from(keccak256(input))
    })
}

async fn insert_manifest(
    pool: &PgPool,
    chain_id: &str,
    source_family: &str,
    file_path: &str,
    payload: serde_json::Value,
) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "
        INSERT INTO manifest_versions (
            manifest_version, namespace, source_family, chain_id, deployment_label,
            rollout_status, normalizer_version, file_path, manifest_payload
        )
        VALUES (1, 'ens', $2, $1, 'fixture', 'active', $3, $4, $5)
        RETURNING manifest_id
        ",
    )
    .bind(chain_id)
    .bind(source_family)
    .bind(NORMALIZER)
    .bind(file_path)
    .bind(payload)
    .fetch_one(pool)
    .await?)
}

async fn insert_log(
    pool: &PgPool,
    chain_id: &str,
    transaction_hash: &str,
    log_index: i64,
    emitting_address: &str,
    topics: &[B256],
    data: &[u8],
) -> Result<()> {
    insert_log_at(
        pool,
        chain_id,
        1,
        transaction_hash,
        log_index,
        emitting_address,
        topics,
        data,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn insert_log_at(
    pool: &PgPool,
    chain_id: &str,
    block_number: i64,
    transaction_hash: &str,
    log_index: i64,
    emitting_address: &str,
    topics: &[B256],
    data: &[u8],
) -> Result<()> {
    let topics = topics
        .iter()
        .map(|topic| format!("{topic:#x}"))
        .collect::<Vec<_>>();
    sqlx::query(
        "
        INSERT INTO raw_logs (
            chain_id, block_hash, block_number, transaction_hash, transaction_index,
            log_index, emitting_address, topics, data
        )
        VALUES ($1, $2, $3, $4, 0, $5, $6, $7, $8)
        ",
    )
    .bind(chain_id)
    .bind(block_hash(chain_id, block_number))
    .bind(block_number)
    .bind(transaction_hash)
    .bind(log_index)
    .bind(emitting_address)
    .bind(topics)
    .bind(data)
    .execute(pool)
    .await?;
    Ok(())
}

fn versioned_token(label: &str, version: u32) -> U256 {
    let mut bytes = *keccak256(label.as_bytes());
    bytes[28..].copy_from_slice(&version.to_be_bytes());
    U256::from_be_bytes(bytes)
}

async fn insert_orphaned_registration(
    pool: &PgPool,
    chain_id: &str,
    block: i64,
    label: &str,
) -> Result<()> {
    insert_orphaned_registration_with_parent(
        pool,
        chain_id,
        block,
        label,
        &block_hash(chain_id, block - 1),
    )
    .await
}

async fn insert_orphaned_registration_with_parent(
    pool: &PgPool,
    chain_id: &str,
    block: i64,
    label: &str,
    parent_hash: &str,
) -> Result<()> {
    let orphan_hash = format!("{chain_id}-orphan-{block}");
    sqlx::query(
        "
        INSERT INTO chain_lineage (
            chain_id, block_hash, parent_hash, block_number, block_timestamp,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, to_timestamp($4), 'orphaned')
        ",
    )
    .bind(chain_id)
    .bind(&orphan_hash)
    .bind(parent_hash)
    .bind(block)
    .execute(pool)
    .await?;
    let transaction_hash = format!("{chain_id}-orphan-transaction-{block}");
    sqlx::query(
        "
        INSERT INTO raw_transactions (
            chain_id, block_hash, block_number, transaction_hash, transaction_index,
            from_address, to_address
        )
        VALUES ($1, $2, $3, $4, 0, $5, $6)
        ",
    )
    .bind(chain_id)
    .bind(&orphan_hash)
    .bind(block)
    .bind(&transaction_hash)
    .bind(SENDER)
    .bind(CONTRACT)
    .execute(pool)
    .await?;
    let encoded = NameRegistered {
        name: label.to_owned(),
        label: B256::from(keccak256(label.as_bytes())),
        owner: CONTRACT.parse::<Address>()?,
        expires: U256::from(1_000_000u64),
    }
    .encode_log_data();
    let topics = encoded
        .topics()
        .iter()
        .map(|topic| format!("{topic:#x}"))
        .collect::<Vec<_>>();
    sqlx::query(
        "
        INSERT INTO raw_logs (
            chain_id, block_hash, block_number, transaction_hash, transaction_index,
            log_index, emitting_address, topics, data
        )
        VALUES ($1, $2, $3, $4, 0, 0, $5, $6, $7)
        ",
    )
    .bind(chain_id)
    .bind(orphan_hash)
    .bind(block)
    .bind(transaction_hash)
    .bind(CONTRACT)
    .bind(topics)
    .bind(encoded.data.to_vec())
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_fixture(pool: &PgPool, chain_id: &str, labels: &[(i64, &str)]) -> Result<()> {
    seed_fixture_with_timestamps(pool, chain_id, labels, &[]).await
}

async fn seed_fixture_with_timestamps(
    pool: &PgPool,
    chain_id: &str,
    labels: &[(i64, &str)],
    timestamp_overrides: &[(i64, i64)],
) -> Result<()> {
    let through = labels.iter().map(|(block, _)| *block).max().unwrap_or(0);
    for block in 0..=through {
        let block_timestamp = timestamp_overrides
            .iter()
            .find_map(|(candidate, timestamp)| (*candidate == block).then_some(*timestamp))
            .unwrap_or(block);
        sqlx::query(
            "
            INSERT INTO chain_lineage (
                chain_id, block_hash, parent_hash, block_number, block_timestamp,
                canonicality_state
            )
            VALUES ($1, $2, $3, $4, to_timestamp($5), 'canonical')
            ",
        )
        .bind(chain_id)
        .bind(block_hash(chain_id, block))
        .bind((block > 0).then(|| block_hash(chain_id, block - 1)))
        .bind(block)
        .bind(block_timestamp)
        .execute(pool)
        .await?;
    }
    let contract_instance_id = Uuid::new_v4();
    sqlx::query("INSERT INTO contract_instances VALUES ($1, $2, 'contract', '{}'::jsonb, now())")
        .bind(contract_instance_id)
        .bind(chain_id)
        .execute(pool)
        .await?;
    let payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v1_registrar_l1",
        "chain": chain_id,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [{
            "role": "registrar",
            "address": CONTRACT,
            "proxy_kind": "none",
            "implementation": null,
            "start_block": 0
        }],
        "discovery_rules": [],
        "abi": { "events": [{
            "name": "NameRegistered",
            "fragment": "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
            "emitter_roles": ["registrar"],
            "normalized_events": ["RegistrationGranted"]
        }], "calls": [] }
    });
    let manifest_id: i64 = sqlx::query_scalar(
        "
        INSERT INTO manifest_versions (
            manifest_version, namespace, source_family, chain_id, deployment_label,
            rollout_status, normalizer_version, file_path, manifest_payload
        )
        VALUES (1, 'ens', 'ens_v1_registrar_l1', $1, 'fixture', 'active', $2, $3, $4)
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
            contract_instance_id, declared_address, role, proxy_kind, start_block_number
        )
        VALUES ($1, $2, 'contract', 'registrar', $3, $4, 'registrar', 'none', 0)
        ",
    )
    .bind(manifest_id)
    .bind(chain_id)
    .bind(contract_instance_id)
    .bind(CONTRACT)
    .execute(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO contract_instance_addresses (
            contract_instance_id, chain_id, address, active_from_block_number,
            source_manifest_id, provenance
        )
        VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)
        ",
    )
    .bind(contract_instance_id)
    .bind(chain_id)
    .bind(CONTRACT)
    .bind(manifest_id)
    .execute(pool)
    .await?;
    for (block, label) in labels {
        insert_name_registered(pool, chain_id, *block, label).await?;
    }
    Ok(())
}

async fn insert_name_registered(
    pool: &PgPool,
    chain_id: &str,
    block: i64,
    label: &str,
) -> Result<()> {
    let transaction_hash = format!("{chain_id}-transaction-{block}");
    sqlx::query(
        "
        INSERT INTO raw_transactions (
            chain_id, block_hash, block_number, transaction_hash, transaction_index,
            from_address, to_address
        )
        VALUES ($1, $2, $3, $4, 0, $5, $6)
        ",
    )
    .bind(chain_id)
    .bind(block_hash(chain_id, block))
    .bind(block)
    .bind(&transaction_hash)
    .bind(SENDER)
    .bind(CONTRACT)
    .execute(pool)
    .await?;
    let encoded = NameRegistered {
        name: label.to_owned(),
        label: B256::from(keccak256(label.as_bytes())),
        owner: CONTRACT.parse::<Address>()?,
        expires: U256::from(1_000_000u64 + block as u64),
    }
    .encode_log_data();
    let topics = encoded
        .topics()
        .iter()
        .map(|topic| format!("{topic:#x}"))
        .collect::<Vec<_>>();
    sqlx::query(
        "
        INSERT INTO raw_logs (
            chain_id, block_hash, block_number, transaction_hash, transaction_index,
            log_index, emitting_address, topics, data
        )
        VALUES ($1, $2, $3, $4, 0, 0, $5, $6, $7)
        ",
    )
    .bind(chain_id)
    .bind(block_hash(chain_id, block))
    .bind(block)
    .bind(transaction_hash)
    .bind(CONTRACT)
    .bind(topics)
    .bind(encoded.data.to_vec())
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_completed_phase_extent(pool: &PgPool, chain_id: &str, hash: &str) -> Result<()> {
    seed_completed_phase_extent_at(pool, chain_id, 2, hash).await
}

async fn seed_completed_phase_extent_at(
    pool: &PgPool,
    chain_id: &str,
    head: i64,
    hash: &str,
) -> Result<()> {
    let head_hash = block_hash(chain_id, head);
    sqlx::query(
        "
        UPDATE chain_phase_state
        SET phase_status = 'completed',
            current_block_number = $2,
            current_block_hash = $3,
            target_block_number = $2,
            target_block_hash = $3,
            live_handoff_block_number = CASE WHEN phase_name = 'ingest' THEN $2 END,
            live_handoff_block_hash = CASE WHEN phase_name = 'ingest' THEN $3 END,
            input_content_hash = CASE WHEN phase_name = 'interpret' THEN $4 END,
            started_at = now(),
            finished_at = now()
        WHERE chain_id = $1
          AND phase_name IN ('ingest', 'interpret')
        ",
    )
    .bind(chain_id)
    .bind(head)
    .bind(&head_hash)
    .bind(hash)
    .execute(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO ingest_cursors (
            chain_id, source_key, source_kind, seed_basis, start_block_number,
            next_block_number, target_block_number, last_processed_block_number,
            last_processed_block_hash
        )
        VALUES ($1, 'source', 'test', 'ethereum_head', 0, $2, $3, $3, $4)
        ",
    )
    .bind(chain_id)
    .bind(head.saturating_add(1))
    .bind(head)
    .bind(head_hash)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_competing_live_block(pool: &PgPool, chain_id: &str, block: i64) -> Result<()> {
    sqlx::query(
        "
        INSERT INTO chain_lineage (
            chain_id, block_hash, parent_hash, block_number, block_timestamp,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, to_timestamp($4), 'safe')
        ",
    )
    .bind(chain_id)
    .bind(format!("{chain_id}-competing-{block}"))
    .bind((block > 0).then(|| block_hash(chain_id, block - 1)))
    .bind(block)
    .execute(pool)
    .await?;
    Ok(())
}

async fn permit_duplicate_live_heights_for_corruption_test(pool: &PgPool) -> Result<()> {
    sqlx::query("DROP INDEX chain_lineage_readable_height_idx")
        .execute(pool)
        .await?;
    Ok(())
}

async fn pool_with_acquire_gate(
    source: &PgPool,
    gated_acquisition: usize,
) -> Result<(PgPool, Arc<Notify>, Arc<Notify>)> {
    let acquire_count = Arc::new(AtomicUsize::new(0));
    let reached = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let count_for_hook = Arc::clone(&acquire_count);
    let reached_for_hook = Arc::clone(&reached);
    let release_for_hook = Arc::clone(&release);
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .test_before_acquire(false)
        .before_acquire(move |_connection, _metadata| {
            let reached = Arc::clone(&reached_for_hook);
            let release = Arc::clone(&release_for_hook);
            let acquisition = count_for_hook.fetch_add(1, Ordering::SeqCst) + 1;
            Box::pin(async move {
                if acquisition == gated_acquisition {
                    reached.notify_one();
                    release.notified().await;
                }
                Ok(true)
            })
        })
        .connect_with(source.connect_options().as_ref().clone())
        .await?;
    drop(pool.acquire().await?);
    acquire_count.store(0, Ordering::SeqCst);
    Ok((pool, reached, release))
}

async fn wait_for_locked_query(pool: &PgPool, query_pattern: &str) -> Result<()> {
    for _ in 0..200 {
        let waiting: bool = sqlx::query_scalar(
            "
            SELECT EXISTS (
                SELECT 1
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND pid <> pg_backend_pid()
                  AND wait_event_type = 'Lock'
                  AND query LIKE $1
            )
            ",
        )
        .bind(query_pattern)
        .fetch_one(pool)
        .await?;
        if waiting {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    anyhow::bail!("interpretation did not reach locked query matching {query_pattern}")
}

async fn switch_live_lineage(pool: &PgPool, chain_id: &str, live_hash: &str) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query(
        "
        UPDATE chain_lineage
        SET canonicality_state = 'orphaned'::canonicality_state
        WHERE chain_id = $1
          AND block_number = 1
          AND block_hash <> $2
        ",
    )
    .bind(chain_id)
    .bind(live_hash)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "
        UPDATE chain_lineage
        SET canonicality_state = 'canonical'::canonicality_state
        WHERE chain_id = $1
          AND block_number = 1
          AND block_hash = $2
        ",
    )
    .bind(chain_id)
    .bind(live_hash)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(())
}

fn chain_config(chain_id: &str) -> phase_runner::error::RunnerResult<ChainConfig> {
    ChainConfig::new(
        chain_id,
        vec![SourceConfig::new(
            chain_id,
            "source",
            "test",
            SeedBasis::EthereumHead,
            0,
            "http://source.invalid",
        )?],
        false,
    )
}

fn test_timing() -> TimingConfig {
    TimingConfig {
        initial_backoff: Duration::from_millis(1),
        maximum_backoff: Duration::from_millis(4),
        live_poll_interval: Duration::from_millis(1),
    }
}

fn block_hash(chain_id: &str, block: i64) -> String {
    format!("{chain_id}-block-{block}")
}
